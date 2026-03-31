package io.codemine.postgresql.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.codemine.postgresql.BinaryInBinaryOutR2dbcCodec;
import io.codemine.postgresql.BinaryInTextOutR2dbcCodec;
import io.codemine.postgresql.TextInBinaryOutR2dbcCodec;
import io.codemine.postgresql.TextInTextOutR2dbcCodec;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.util.PGobject;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class CodecITBase<A> {

  static final PostgreSQLContainer<?> container;

  static {
    container =
        new PostgreSQLContainer<>("postgres:18").withCommand("postgres -c max_connections=300");
    container.start();
  }

  /**
   * Holds all shared connections for a single concrete test class. Created once per subclass and
   * cached in {@link #sharedConnectionsByClass} so that jqwik's per-property instance creation does
   * not open fresh connections on each instantiation.
   */
  private static class SharedConnections {
    final java.sql.Connection pgjdbcConnection;
    final Connection binaryInBinaryOutConn;
    final Connection textInTextOutConn;
    final Connection textInBinaryOutConn;
    final Connection binaryInTextOutConn;

    SharedConnections(
        java.sql.Connection pgjdbcConnection,
        Connection binaryInBinaryOutConn,
        Connection textInTextOutConn,
        Connection textInBinaryOutConn,
        Connection binaryInTextOutConn) {
      this.pgjdbcConnection = pgjdbcConnection;
      this.binaryInBinaryOutConn = binaryInBinaryOutConn;
      this.textInTextOutConn = textInTextOutConn;
      this.textInBinaryOutConn = textInBinaryOutConn;
      this.binaryInTextOutConn = binaryInTextOutConn;
    }

    void close() throws Exception {
      pgjdbcConnection.close();
      Mono.from(binaryInBinaryOutConn.close())
          .then(Mono.from(textInTextOutConn.close()))
          .then(Mono.from(textInBinaryOutConn.close()))
          .then(Mono.from(binaryInTextOutConn.close()))
          .block();
    }
  }

  /**
   * Cache of shared connections keyed by concrete test class. Ensures that connections are opened
   * exactly once per subclass regardless of how many instances the test engines create.
   */
  private static final ConcurrentHashMap<Class<?>, SharedConnections> sharedConnectionsByClass =
      new ConcurrentHashMap<>();

  private final Codec<A> codec;
  private final Class<A> type;

  private final Codec<List<A>> arrayCodec;

  /** JDBC connection (pgjdbc) used for text-protocol baseline tests. */
  private final java.sql.Connection pgjdbcConnection;

  /**
   * Persistent R2DBC connection whose codec sends parameters in <b>binary</b> format and expects
   * results in <b>binary</b> format ({@code forceBinary=true}). Handles both scalar and array
   * values.
   */
  private final Connection binaryInBinaryOutConn;

  /**
   * Persistent R2DBC connection whose codec sends parameters in <b>text</b> format and expects
   * results in <b>text</b> format (no {@code forceBinary}). Handles both scalar and array values.
   */
  private final Connection textInTextOutConn;

  /**
   * Persistent R2DBC connection whose codec sends parameters in <b>text</b> format and expects
   * results in <b>binary</b> format ({@code forceBinary=true}). Handles both scalar and array
   * values.
   */
  private final Connection textInBinaryOutConn;

  /**
   * Persistent R2DBC connection whose codec sends parameters in <b>binary</b> format and expects
   * results in <b>text</b> format (no {@code forceBinary}). Handles both scalar and array values.
   */
  private final Connection binaryInTextOutConn;

  @SuppressWarnings("unchecked")
  protected CodecITBase(Codec<A> codec, Class<A> type) {
    this.codec = codec;
    this.type = type;
    this.arrayCodec = codec.inDim();

    // Retrieve or create shared connections for this concrete subclass.
    // computeIfAbsent ensures that even when jqwik instantiates the class
    // multiple times (once per @Property), we only ever open the 5
    // connections once.
    SharedConnections conns =
        sharedConnectionsByClass.computeIfAbsent(
            this.getClass(), cls -> createSharedConnections(codec, type));

    pgjdbcConnection = conns.pgjdbcConnection;
    binaryInBinaryOutConn = conns.binaryInBinaryOutConn;
    textInTextOutConn = conns.textInTextOutConn;
    textInBinaryOutConn = conns.textInBinaryOutConn;
    binaryInTextOutConn = conns.binaryInTextOutConn;
  }

  @SuppressWarnings("unchecked")
  private SharedConnections createSharedConnections(Codec<A> codec, Class<A> type) {
    java.sql.Connection pgjdbc;
    try {
      var props = new java.util.Properties();
      props.setProperty("user", container.getUsername());
      props.setProperty("password", container.getPassword());
      // Disable server-side prepared-statement caching so that all result
      // columns remain in text format (avoids rs.getString() returning
      // "[B@…" for bytea columns after the binary-mode switch threshold).
      props.setProperty("prepareThreshold", "0");
      pgjdbc = DriverManager.getConnection(container.getJdbcUrl(), props);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to open connection", e);
    }

    Class<List<A>> listClass = (Class<List<A>>) (Class<?>) List.class;
    Codec<List<A>> arrayCd = codec.inDim();

    // Each connection handles both the scalar and array codec.
    return new SharedConnections(
        pgjdbc,
        r2dbcConnect(
            true,
            new BinaryInBinaryOutR2dbcCodec<>(codec, type),
            new BinaryInBinaryOutR2dbcCodec<>(arrayCd, listClass)),
        r2dbcConnect(
            false,
            new TextInTextOutR2dbcCodec<>(codec, type),
            new TextInTextOutR2dbcCodec<>(arrayCd, listClass)),
        r2dbcConnect(
            true,
            new TextInBinaryOutR2dbcCodec<>(codec, type),
            new TextInBinaryOutR2dbcCodec<>(arrayCd, listClass)),
        r2dbcConnect(
            false,
            new BinaryInTextOutR2dbcCodec<>(codec, type),
            new BinaryInTextOutR2dbcCodec<>(arrayCd, listClass)));
  }

  @AfterAll
  void closeConnections() throws Exception {
    SharedConnections conns = sharedConnectionsByClass.remove(this.getClass());
    if (conns != null) {
      conns.close();
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------
  private Connection r2dbcConnect(
      boolean forceBinary, io.r2dbc.postgresql.codec.Codec<?>... r2dbcCodecs) {
    var builder =
        PostgresqlConnectionConfiguration.builder()
            .host(container.getHost())
            .port(container.getMappedPort(5432))
            .username(container.getUsername())
            .password(container.getPassword())
            .database(container.getDatabaseName())
            .codecRegistrar(
                (c, allocator, registry) -> {
                  for (var r2dbcCodec : r2dbcCodecs) {
                    registry.addFirst(r2dbcCodec);
                  }
                  return Mono.empty();
                });
    if (forceBinary) {
      builder.forceBinary(true);
    }
    return Mono.from(new PostgresqlConnectionFactory(builder.build()).create()).block();
  }

  private A roundtripViaR2dbc(Connection r2conn, A value) {
    return Flux.from(r2conn.createStatement("SELECT $1").bind(0, value).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, type)))
        .single()
        .block();
  }

  @SuppressWarnings("unchecked")
  private List<A> roundtripArrayViaR2dbc(Connection r2conn, List<A> value) {
    return (List<A>)
        Flux.from(r2conn.createStatement("SELECT $1").bind(0, value).execute())
            .flatMap(result -> result.map((row, meta) -> row.get(0, List.class)))
            .single()
            .block();
  }

  @Provide
  Arbitrary<A> values() {
    return net.jqwik.api.Arbitraries.fromGeneratorWithSize(
        size -> r -> net.jqwik.api.Shrinkable.unshrinkable(codec.random(r, size)));
  }

  @Provide
  Arbitrary<List<A>> arrayValues() {
    return net.jqwik.api.Arbitraries.fromGeneratorWithSize(
        size -> r -> net.jqwik.api.Shrinkable.unshrinkable(arrayCodec.random(r, size)));
  }

  // -----------------------------------------------------------------------
  // Scalar tests
  // -----------------------------------------------------------------------
  @Test
  void oidMatchesName() throws Exception {
    if (codec.scalarOid() == 0) {
      return;
    }
    try (var ps =
        pgjdbcConnection.prepareStatement("SELECT oid::int FROM pg_type WHERE typname = ?")) {
      // TODO: Account for schema-qualified types names
      ps.setString(1, codec.name());
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          assertEquals(rs.getInt(1), codec.scalarOid(), "OID mismatch for " + codec.name());
        }
      }
    }
  }

  @Test
  void arrayOidMatchesName() throws Exception {
    if (arrayCodec.arrayOid() == 0) {
      return;
    }
    // Array types in pg_type are named "_<element_name>".
    try (var ps =
        pgjdbcConnection.prepareStatement("SELECT oid::int FROM pg_type WHERE typname = ?")) {
      ps.setString(1, "_" + codec.name());
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          assertEquals(
              rs.getInt(1),
              arrayCodec.arrayOid(),
              "Array OID mismatch for " + arrayCodec.typeSig());
        }
      }
    }
  }

  @Property(tries = 100)
  void roundtripsInBinaryToBinaryViaR2dbc(@ForAll("values") A value) throws Exception {
    A decoded = roundtripViaR2dbc(binaryInBinaryOutConn, value);
    assertEquals(value, decoded, "decode mismatch for " + codec.typeSig() + " value=" + value);
  }

  @Property(tries = 100)
  void roundtripsInTextToTextViaR2dbc(@ForAll("values") A value) throws Exception {
    A decoded = roundtripViaR2dbc(textInTextOutConn, value);
    assertEquals(value, decoded, "decode mismatch for " + codec.typeSig() + " value=" + value);
  }

  @Property(tries = 100)
  void roundtripsInTextToBinaryViaR2dbc(@ForAll("values") A value) throws Exception {
    A decoded = roundtripViaR2dbc(textInBinaryOutConn, value);
    assertEquals(value, decoded, "decode mismatch for " + codec.typeSig() + " value=" + value);
  }

  @Property(tries = 100)
  void roundtripsInBinaryToTextViaR2dbc(@ForAll("values") A value) throws Exception {
    A decoded = roundtripViaR2dbc(binaryInTextOutConn, value);
    assertEquals(value, decoded, "decode mismatch for " + codec.typeSig() + " value=" + value);
  }

  @Property(tries = 100)
  void roundtripsInTextToTextViaPgjdbc(@ForAll("values") A value) throws Exception {
    try (var ps = pgjdbcConnection.prepareStatement("SELECT ?")) {
      if (value != null) {
        PGobject obj = new PGobject();
        obj.setType(qualifiedCodecName(codec));
        {
          StringBuilder sb = new StringBuilder();
          codec.encodeInText(sb, value);
          obj.setValue(sb.toString());
        }
        ps.setObject(1, obj);
      } else {
        ps.setNull(1, java.sql.Types.OTHER);
      }

      A decoded;
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next(), "Expected a result row");
        String text = rs.getString(1);
        if (text == null) {
          decoded = null;
        } else {
          var result = codec.decodeInText(text, 0);
          decoded = result.value;
        }
      }

      assertEquals(value, decoded, "decode mismatch for " + codec.typeSig() + " value=" + value);
    }
  }

  // -----------------------------------------------------------------------
  // Array tests
  // -----------------------------------------------------------------------

  @Property(tries = 100)
  void arrayRoundtripsInBinaryToBinaryViaR2dbc(@ForAll("arrayValues") List<A> value)
      throws Exception {
    List<A> decoded = roundtripArrayViaR2dbc(binaryInBinaryOutConn, value);
    assertEquals(value, decoded, "decode mismatch for " + arrayCodec.typeSig() + " value=" + value);
  }

  @Property(tries = 100)
  void arrayRoundtripsInTextToTextViaR2dbc(@ForAll("arrayValues") List<A> value) throws Exception {
    List<A> decoded = roundtripArrayViaR2dbc(textInTextOutConn, value);
    assertEquals(value, decoded, "decode mismatch for " + arrayCodec.typeSig() + " value=" + value);
  }

  @Property(tries = 100)
  void arrayRoundtripsInTextToBinaryViaR2dbc(@ForAll("arrayValues") List<A> value)
      throws Exception {
    List<A> decoded = roundtripArrayViaR2dbc(textInBinaryOutConn, value);
    assertEquals(value, decoded, "decode mismatch for " + arrayCodec.typeSig() + " value=" + value);
  }

  @Property(tries = 100)
  void arrayRoundtripsInBinaryToTextViaR2dbc(@ForAll("arrayValues") List<A> value)
      throws Exception {
    List<A> decoded = roundtripArrayViaR2dbc(binaryInTextOutConn, value);
    assertEquals(value, decoded, "decode mismatch for " + arrayCodec.typeSig() + " value=" + value);
  }

  @Property(tries = 100)
  void arrayRoundtripsInTextToTextViaPgjdbc(@ForAll("arrayValues") List<A> value) throws Exception {
    try (var ps = pgjdbcConnection.prepareStatement("SELECT ?")) {
      if (value != null) {
        PGobject obj = new PGobject();
        obj.setType(qualifiedCodecName(arrayCodec));
        {
          StringBuilder sb = new StringBuilder();
          arrayCodec.encodeInText(sb, value);
          obj.setValue(sb.toString());
        }
        ps.setObject(1, obj);
      } else {
        ps.setNull(1, java.sql.Types.OTHER);
      }

      List<A> decoded;
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next(), "Expected a result row");
        String text = rs.getString(1);
        if (text == null) {
          decoded = null;
        } else {
          decoded = arrayCodec.decodeInText(text, 0).value;
        }
      }

      assertEquals(
          value, decoded, "decode mismatch for " + arrayCodec.typeSig() + " value=" + value);
    }
  }

  private static String qualifiedCodecName(Codec codec) {
    StringBuilder sb = new StringBuilder();
    if (codec.schema() != null && !codec.schema().isEmpty()) {
      sb.append(codec.schema()).append(".");
    }
    sb.append(codec.name());
    for (int i = 0; i < codec.dimensions(); i++) {
      sb.append("[]");
    }
    return sb.toString();
  }
}
