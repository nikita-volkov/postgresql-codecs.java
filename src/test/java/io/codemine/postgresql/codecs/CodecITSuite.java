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
abstract class CodecITSuite<A> {

  static final PostgreSQLContainer<?> pg;

  static {
    pg = new PostgreSQLContainer<>("postgres:18");
    pg.start();
  }

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
  protected CodecITSuite(Codec<A> codec, Class<A> type) {
    this.codec = codec;
    this.type = type;
    this.arrayCodec = codec.inDim();

    try {
      var props = new java.util.Properties();
      props.setProperty("user", pg.getUsername());
      props.setProperty("password", pg.getPassword());
      // Disable server-side prepared-statement caching so that all result
      // columns remain in text format (avoids rs.getString() returning
      // "[B@…" for bytea columns after the binary-mode switch threshold).
      props.setProperty("prepareThreshold", "0");
      pgjdbcConnection = DriverManager.getConnection(pg.getJdbcUrl(), props);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to open connection", e);
    }

    Class<List<A>> listClass = (Class<List<A>>) (Class<?>) List.class;

    // Each connection handles both the scalar and array codec, so no extra connections are needed.
    binaryInBinaryOutConn =
        r2dbcConnect(
            true,
            new BinaryInBinaryOutR2dbcCodec<>(codec, type),
            new BinaryInBinaryOutR2dbcCodec<>(arrayCodec, listClass));
    textInTextOutConn =
        r2dbcConnect(
            false,
            new TextInTextOutR2dbcCodec<>(codec, type),
            new TextInTextOutR2dbcCodec<>(arrayCodec, listClass));
    textInBinaryOutConn =
        r2dbcConnect(
            true,
            new TextInBinaryOutR2dbcCodec<>(codec, type),
            new TextInBinaryOutR2dbcCodec<>(arrayCodec, listClass));
    binaryInTextOutConn =
        r2dbcConnect(
            false,
            new BinaryInTextOutR2dbcCodec<>(codec, type),
            new BinaryInTextOutR2dbcCodec<>(arrayCodec, listClass));
  }

  @AfterAll
  void closeConnections() throws Exception {
    pgjdbcConnection.close();
    Mono.from(binaryInBinaryOutConn.close())
        .then(Mono.from(textInTextOutConn.close()))
        .then(Mono.from(textInBinaryOutConn.close()))
        .then(Mono.from(binaryInTextOutConn.close()))
        .block();
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------
  private Connection r2dbcConnect(
      boolean forceBinary, io.r2dbc.postgresql.codec.Codec<?>... r2dbcCodecs) {
    var builder =
        PostgresqlConnectionConfiguration.builder()
            .host(pg.getHost())
            .port(pg.getMappedPort(5432))
            .username(pg.getUsername())
            .password(pg.getPassword())
            .database(pg.getDatabaseName())
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
    return Flux.from(
            r2conn.createStatement("SELECT $1::" + codec.typeSig()).bind(0, value).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, type)))
        .single()
        .block();
  }

  @SuppressWarnings("unchecked")
  private List<A> roundtripArrayViaR2dbc(Connection r2conn, List<A> value) {
    return (List<A>)
        Flux.from(
                r2conn
                    .createStatement("SELECT $1::" + arrayCodec.typeSig())
                    .bind(0, value)
                    .execute())
            .flatMap(result -> result.map((row, meta) -> row.get(0, List.class)))
            .single()
            .block();
  }

  @Provide
  Arbitrary<A> values() {
    return net.jqwik.api.Arbitraries.randomValue(codec::random);
  }

  @Provide
  Arbitrary<List<A>> arrayValues() {
    return net.jqwik.api.Arbitraries.randomValue(arrayCodec::random);
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
    try (var ps = pgjdbcConnection.prepareStatement("SELECT ?::" + codec.typeSig())) {
      if (value != null) {
        PGobject obj = new PGobject();
        obj.setType(codec.typeSig());
        {
          StringBuilder sb = new StringBuilder();
          codec.write(sb, value);
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
          var result = codec.parse(text, 0);
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
    try (var ps = pgjdbcConnection.prepareStatement("SELECT ?::" + arrayCodec.typeSig())) {
      if (value != null) {
        PGobject obj = new PGobject();
        obj.setType(arrayCodec.typeSig());
        {
          StringBuilder sb = new StringBuilder();
          arrayCodec.write(sb, value);
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
          decoded = arrayCodec.parse(text, 0).value;
        }
      }

      assertEquals(
          value, decoded, "decode mismatch for " + arrayCodec.typeSig() + " value=" + value);
    }
  }
}
