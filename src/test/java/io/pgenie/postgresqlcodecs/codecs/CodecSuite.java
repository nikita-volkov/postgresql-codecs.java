package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.pgenie.postgresqlcodecs.BinaryInBinaryOutR2dbcCodec;
import io.pgenie.postgresqlcodecs.BinaryInTextOutR2dbcCodec;
import io.pgenie.postgresqlcodecs.TextInBinaryOutR2dbcCodec;
import io.pgenie.postgresqlcodecs.TextInTextOutR2dbcCodec;
import io.pgenie.postgresqlcodecs.arbitrary.Arbitrary;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.util.PGobject;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class CodecSuite<A> {

  static final PostgreSQLContainer<?> pg;

  static {
    pg = new PostgreSQLContainer<>("postgres:18");
    pg.start();
  }

  private final Codec<A> codec;
  private final Arbitrary<A> arbitrary;
  private final Class<A> type;

  /** JDBC connection (pgjdbc) used for text-protocol baseline tests. */
  private final java.sql.Connection conn;

  /**
   * R2DBC connection factory whose codec sends parameters in <b>binary</b> format and expects
   * results in <b>binary</b> format ({@code forceBinary=true}).
   */
  private final PostgresqlConnectionFactory binaryInBinaryOutFactory;

  /**
   * R2DBC connection factory whose codec sends parameters in <b>text</b> format and expects results
   * in <b>text</b> format (no {@code forceBinary}).
   */
  private final PostgresqlConnectionFactory textInTextOutFactory;

  /**
   * R2DBC connection factory whose codec sends parameters in <b>text</b> format and expects results
   * in <b>binary</b> format ({@code forceBinary=true}).
   */
  private final PostgresqlConnectionFactory textInBinaryOutFactory;

  /**
   * R2DBC connection factory whose codec sends parameters in <b>binary</b> format and expects
   * results in <b>text</b> format (no {@code forceBinary}).
   */
  private final PostgresqlConnectionFactory binaryInTextOutFactory;

  protected CodecSuite(Codec<A> codec, Arbitrary<A> arbitrary, Class<A> type) {
    this.codec = codec;
    this.arbitrary = arbitrary;
    this.type = type;

    try {
      var props = new java.util.Properties();
      props.setProperty("user", pg.getUsername());
      props.setProperty("password", pg.getPassword());
      // Disable server-side prepared-statement caching so that all result
      // columns remain in text format (avoids rs.getString() returning
      // "[B@…" for bytea columns after the binary-mode switch threshold).
      props.setProperty("prepareThreshold", "0");
      conn = DriverManager.getConnection(pg.getJdbcUrl(), props);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to open connection", e);
    }

    binaryInBinaryOutFactory = r2dbcFactory(true, new BinaryInBinaryOutR2dbcCodec<>(codec, type));
    textInTextOutFactory = r2dbcFactory(false, new TextInTextOutR2dbcCodec<>(codec, type));
    textInBinaryOutFactory = r2dbcFactory(true, new TextInBinaryOutR2dbcCodec<>(codec, type));
    binaryInTextOutFactory = r2dbcFactory(false, new BinaryInTextOutR2dbcCodec<>(codec, type));
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------
  private PostgresqlConnectionFactory r2dbcFactory(
      boolean forceBinary, io.r2dbc.postgresql.codec.Codec<A> r2dbcCodec) {
    var builder =
        PostgresqlConnectionConfiguration.builder()
            .host(pg.getHost())
            .port(pg.getMappedPort(5432))
            .username(pg.getUsername())
            .password(pg.getPassword())
            .database(pg.getDatabaseName())
            .codecRegistrar(
                (c, allocator, registry) -> {
                  registry.addFirst(r2dbcCodec);
                  return Mono.empty();
                });
    if (forceBinary) {
      builder.forceBinary(true);
    }
    return new PostgresqlConnectionFactory(builder.build());
  }

  private A roundtripViaR2dbc(PostgresqlConnectionFactory factory, A value) {
    return Mono.usingWhen(
            factory.create(),
            r2conn ->
                Flux.from(
                        r2conn
                            .createStatement("SELECT $1::" + codec.typeSig())
                            .bind(0, value)
                            .execute())
                    .flatMap(result -> result.map((row, meta) -> row.get(0, type)))
                    .single(),
            c -> c.close())
        .block();
  }

  private Stream<Arguments> factory() {
    return Arbitrary.samples(arbitrary);
  }

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------
  @Test
  void oidMatchesName() throws Exception {
    if (codec.oid() == 0) {
      return;
    }
    try (var ps = conn.prepareStatement("SELECT oid::int FROM pg_type WHERE typname = ?")) {
      // TODO: Account for schema-qualified types names
      ps.setString(1, codec.name());
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          assertEquals(rs.getInt(1), codec.oid(), "OID mismatch for " + codec.name());
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("factory")
  void roundtripsInBinaryToBinaryViaR2dbc(A value) throws Exception {
    A decoded = roundtripViaR2dbc(binaryInBinaryOutFactory, value);
    assertEquals(value, decoded, "decode mismatch for " + codec.typeSig() + " value=" + value);
  }

  @ParameterizedTest
  @MethodSource("factory")
  void roundtripsInTextToTextViaR2dbc(A value) throws Exception {
    A decoded = roundtripViaR2dbc(textInTextOutFactory, value);
    assertEquals(value, decoded, "decode mismatch for " + codec.typeSig() + " value=" + value);
  }

  @ParameterizedTest
  @MethodSource("factory")
  void roundtripsInTextToBinaryViaR2dbc(A value) throws Exception {
    A decoded = roundtripViaR2dbc(textInBinaryOutFactory, value);
    assertEquals(value, decoded, "decode mismatch for " + codec.typeSig() + " value=" + value);
  }

  @ParameterizedTest
  @MethodSource("factory")
  void roundtripsInBinaryToTextViaR2dbc(A value) throws Exception {
    A decoded = roundtripViaR2dbc(binaryInTextOutFactory, value);
    assertEquals(value, decoded, "decode mismatch for " + codec.typeSig() + " value=" + value);
  }

  @ParameterizedTest
  @MethodSource("factory")
  void roundtripsInTextToTextViaPgjdbc(A value) throws Exception {
    try (var ps = conn.prepareStatement("SELECT ?::" + codec.typeSig())) {
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
}
