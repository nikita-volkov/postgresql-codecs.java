package io.codemine.postgresql.codecs;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;
import org.testcontainers.containers.PostgreSQLContainer;

import io.codemine.postgresql.TextInBinaryOutR2dbcCodec;
import io.codemine.postgresql.TextInTextOutR2dbcCodec;
import io.codemine.postgresql.codecs.CompositeCodecTest.AnnotatedSegment;
import io.codemine.postgresql.codecs.CompositeCodecTest.Point;
import io.codemine.postgresql.codecs.CompositeCodecTest.Segment;
import io.codemine.postgresql.codecs.CompositeCodecTest.TaggedData;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Integration tests for {@link CompositeCodec}, exercising four composite scenarios against a real
 * PostgreSQL instance. Each scenario is tested via pgjdbc (text), R2DBC text-to-text, and R2DBC
 * text-to-binary (sends text params, receives binary rows).
 *
 * <p>The Docker container is shared via a {@code static} field. JDBC and R2DBC connections are
 * per-instance (constructor-initialized) so that jqwik's per-property instance re-creation always
 * finds live connections. The DDL is guarded by an {@code AtomicBoolean} so composite types are
 * created only once.
 */
class CompositeCodecIT {

  // -----------------------------------------------------------------------
  // Shared container (started once)
  // -----------------------------------------------------------------------
  static final PostgreSQLContainer<?> CONTAINER;
  private static final AtomicBoolean DDL_DONE = new AtomicBoolean(false);

  static {
    CONTAINER = new PostgreSQLContainer<>("postgres:18");
    CONTAINER.start();
  }

  // -----------------------------------------------------------------------
  // Per-instance connections
  // -----------------------------------------------------------------------
  private final java.sql.Connection pgjdbcConn;

  /** R2DBC: text params, text results (scalar codecs only). */
  private final Connection textInTextOutConn;

  /** R2DBC forceBinary: text params, binary results (scalar codecs only). */
  private final Connection textInBinaryOutConn;

  /**
   * Dedicated connection for Point-array roundtrips (one array codec avoids List dispatch
   * ambiguity).
   */
  private final Connection pointArrayTextConn;

  private final Connection pointArrayBinaryConn;

  @SuppressWarnings({"unchecked", "rawtypes"})
  CompositeCodecIT() {
    try {
      var props = new java.util.Properties();
      props.setProperty("user", CONTAINER.getUsername());
      props.setProperty("password", CONTAINER.getPassword());
      props.setProperty("prepareThreshold", "0");
      pgjdbcConn = DriverManager.getConnection(CONTAINER.getJdbcUrl(), props);

      // Create types in dependency order (leaf types first), only once.
      if (DDL_DONE.compareAndSet(false, true)) {
        try (var stmt = pgjdbcConn.createStatement()) {
          stmt.execute("CREATE TYPE test_pt     AS (x int4, y int4)");
          stmt.execute("CREATE TYPE test_seg    AS (start_pt test_pt, end_pt test_pt)");
          stmt.execute("CREATE TYPE test_tagged AS (tag text, items text[])");
          stmt.execute("CREATE TYPE test_ann_seg AS (label text, seg test_seg, tags text[])");
        }
      }

      // Scalar-only connections: no array codecs to avoid List.class dispatch ambiguity.
      textInTextOutConn =
          connect(
              false,
              new TextInTextOutR2dbcCodec<>(CompositeCodecTest.POINT_CODEC, Point.class),
              new TextInTextOutR2dbcCodec<>(CompositeCodecTest.SEGMENT_CODEC, Segment.class),
              new TextInTextOutR2dbcCodec<>(CompositeCodecTest.TAGGED_DATA_CODEC, TaggedData.class),
              new TextInTextOutR2dbcCodec<>(
                  CompositeCodecTest.ANNOTATED_CODEC, AnnotatedSegment.class));

      textInBinaryOutConn =
          connect(
              true,
              new TextInBinaryOutR2dbcCodec<>(CompositeCodecTest.POINT_CODEC, Point.class),
              new TextInBinaryOutR2dbcCodec<>(CompositeCodecTest.SEGMENT_CODEC, Segment.class),
              new TextInBinaryOutR2dbcCodec<>(
                  CompositeCodecTest.TAGGED_DATA_CODEC, TaggedData.class),
              new TextInBinaryOutR2dbcCodec<>(
                  CompositeCodecTest.ANNOTATED_CODEC, AnnotatedSegment.class));

      // Dedicated Point-array connections: single array codec avoids List.class ambiguity.
      pointArrayTextConn =
          connect(
              false,
              new TextInTextOutR2dbcCodec<>(CompositeCodecTest.POINT_CODEC, Point.class),
              new TextInTextOutR2dbcCodec(CompositeCodecTest.POINT_CODEC.inDim(), List.class));
      pointArrayBinaryConn =
          connect(
              true,
              new TextInBinaryOutR2dbcCodec<>(CompositeCodecTest.POINT_CODEC, Point.class),
              new TextInBinaryOutR2dbcCodec(CompositeCodecTest.POINT_CODEC.inDim(), List.class));
    } catch (Exception e) {
      throw new RuntimeException("CompositeCodecIT setup failed", e);
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------
  private Connection connect(boolean forceBinary, io.r2dbc.postgresql.codec.Codec<?>... codecs) {
    var builder =
        PostgresqlConnectionConfiguration.builder()
            .host(CONTAINER.getHost())
            .port(CONTAINER.getMappedPort(5432))
            .username(CONTAINER.getUsername())
            .password(CONTAINER.getPassword())
            .database(CONTAINER.getDatabaseName())
            .codecRegistrar(
                (c, allocator, registry) -> {
                  for (var codec : codecs) {
                    registry.addFirst(codec);
                  }
                  return Mono.empty();
                });
    if (forceBinary) {
      builder.forceBinary(true);
    }
    return Mono.from(new PostgresqlConnectionFactory(builder.build()).create()).block();
  }

  private <A> A roundtripViaR2dbc(Connection conn, Codec<A> codec, A value, Class<A> type) {
    return Flux.from(conn.createStatement("SELECT $1::" + codec.typeSig()).bind(0, value).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, type)))
        .single()
        .block();
  }

  @SuppressWarnings("unchecked")
  private <A> List<A> arrayRoundtripViaR2dbc(Connection conn, Codec<A> scalar, List<A> value) {
    Codec<List<A>> arrayCodec = scalar.inDim();
    return (List<A>)
        Flux.from(
                conn.createStatement("SELECT $1::" + arrayCodec.typeSig()).bind(0, value).execute())
            .flatMap(result -> result.map((row, meta) -> row.get(0, List.class)))
            .single()
            .block();
  }

  private <A> A roundtripViaPgjdbc(Codec<A> codec, A value) throws Exception {
    try (var ps = pgjdbcConn.prepareStatement("SELECT ?::" + codec.typeSig())) {
      PGobject obj = new PGobject();
      obj.setType(codec.typeSig());
      StringBuilder sb = new StringBuilder();
      codec.write(sb, value);
      obj.setValue(sb.toString());
      ps.setObject(1, obj);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next(), "Expected a result row");
        return codec.parse(rs.getString(1), 0).value;
      }
    }
  }

  // -----------------------------------------------------------------------
  // Catalog sanity check
  // -----------------------------------------------------------------------
  @Test
  void compositeTypesExistInCatalog() throws Exception {
    for (String typeName : List.of("test_pt", "test_seg", "test_tagged", "test_ann_seg")) {
      try (var ps = pgjdbcConn.prepareStatement("SELECT oid FROM pg_type WHERE typname = ?")) {
        ps.setString(1, typeName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next(), typeName + " not found in pg_type");
        }
      }
    }
  }

  // -----------------------------------------------------------------------
  // Providers
  // -----------------------------------------------------------------------
  @Provide("points")
  Arbitrary<Point> points() {
    return Arbitraries.randomValue(CompositeCodecTest.POINT_CODEC::random);
  }

  @Provide("segments")
  Arbitrary<Segment> segments() {
    return Arbitraries.randomValue(CompositeCodecTest.SEGMENT_CODEC::random);
  }

  @Provide("taggedData")
  Arbitrary<TaggedData> taggedData() {
    return Arbitraries.randomValue(CompositeCodecTest.TAGGED_DATA_CODEC::random);
  }

  @Provide("annotatedSegments")
  Arbitrary<AnnotatedSegment> annotatedSegments() {
    return Arbitraries.randomValue(CompositeCodecTest.ANNOTATED_CODEC::random);
  }

  @Provide("pointArrays")
  Arbitrary<List<Point>> pointArrays() {
    return Arbitraries.randomValue(CompositeCodecTest.POINT_CODEC.inDim()::random);
  }

  // -----------------------------------------------------------------------
  // Simple 2-field composite: (x int4, y int4)
  // -----------------------------------------------------------------------
  @Property(tries = 50)
  void point_pgjdbc(@ForAll("points") Point v) throws Exception {
    assertEquals(v, roundtripViaPgjdbc(CompositeCodecTest.POINT_CODEC, v));
  }

  @Property(tries = 50)
  void point_r2dbc_textToText(@ForAll("points") Point v) {
    assertEquals(
        v, roundtripViaR2dbc(textInTextOutConn, CompositeCodecTest.POINT_CODEC, v, Point.class));
  }

  @Property(tries = 50)
  void point_r2dbc_textToBinary(@ForAll("points") Point v) {
    assertEquals(
        v, roundtripViaR2dbc(textInBinaryOutConn, CompositeCodecTest.POINT_CODEC, v, Point.class));
  }

  @Property(tries = 50)
  void pointArray_r2dbc_textToText(@ForAll("pointArrays") List<Point> v) {
    assertEquals(v, arrayRoundtripViaR2dbc(pointArrayTextConn, CompositeCodecTest.POINT_CODEC, v));
  }

  @Property(tries = 50)
  void pointArray_r2dbc_textToBinary(@ForAll("pointArrays") List<Point> v) {
    assertEquals(
        v, arrayRoundtripViaR2dbc(pointArrayBinaryConn, CompositeCodecTest.POINT_CODEC, v));
  }

  // -----------------------------------------------------------------------
  // Nested composite: (start test_pt, end test_pt)
  // -----------------------------------------------------------------------
  @Property(tries = 50)
  void segment_pgjdbc(@ForAll("segments") Segment v) throws Exception {
    assertEquals(v, roundtripViaPgjdbc(CompositeCodecTest.SEGMENT_CODEC, v));
  }

  @Property(tries = 50)
  void segment_r2dbc_textToText(@ForAll("segments") Segment v) {
    assertEquals(
        v,
        roundtripViaR2dbc(textInTextOutConn, CompositeCodecTest.SEGMENT_CODEC, v, Segment.class));
  }

  @Property(tries = 50)
  void segment_r2dbc_textToBinary(@ForAll("segments") Segment v) {
    assertEquals(
        v,
        roundtripViaR2dbc(textInBinaryOutConn, CompositeCodecTest.SEGMENT_CODEC, v, Segment.class));
  }

  // -----------------------------------------------------------------------
  // Composite with array field: (tag text, items text[])
  // -----------------------------------------------------------------------
  @Property(tries = 50)
  void taggedData_pgjdbc(@ForAll("taggedData") TaggedData v) throws Exception {
    assertEquals(v, roundtripViaPgjdbc(CompositeCodecTest.TAGGED_DATA_CODEC, v));
  }

  @Property(tries = 50)
  void taggedData_r2dbc_textToText(@ForAll("taggedData") TaggedData v) {
    assertEquals(
        v,
        roundtripViaR2dbc(
            textInTextOutConn, CompositeCodecTest.TAGGED_DATA_CODEC, v, TaggedData.class));
  }

  @Property(tries = 50)
  void taggedData_r2dbc_textToBinary(@ForAll("taggedData") TaggedData v) {
    assertEquals(
        v,
        roundtripViaR2dbc(
            textInBinaryOutConn, CompositeCodecTest.TAGGED_DATA_CODEC, v, TaggedData.class));
  }

  // -----------------------------------------------------------------------
  // Composite with nested composite + array: (label text, seg test_seg, tags text[])
  // -----------------------------------------------------------------------
  @Property(tries = 50)
  void annotatedSegment_pgjdbc(@ForAll("annotatedSegments") AnnotatedSegment v) throws Exception {
    assertEquals(v, roundtripViaPgjdbc(CompositeCodecTest.ANNOTATED_CODEC, v));
  }

  @Property(tries = 50)
  void annotatedSegment_r2dbc_textToText(@ForAll("annotatedSegments") AnnotatedSegment v) {
    assertEquals(
        v,
        roundtripViaR2dbc(
            textInTextOutConn, CompositeCodecTest.ANNOTATED_CODEC, v, AnnotatedSegment.class));
  }

  @Property(tries = 50)
  void annotatedSegment_r2dbc_textToBinary(@ForAll("annotatedSegments") AnnotatedSegment v) {
    assertEquals(
        v,
        roundtripViaR2dbc(
            textInBinaryOutConn, CompositeCodecTest.ANNOTATED_CODEC, v, AnnotatedSegment.class));
  }
}
