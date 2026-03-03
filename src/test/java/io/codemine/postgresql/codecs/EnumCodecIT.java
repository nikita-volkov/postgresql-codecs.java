package io.codemine.postgresql.codecs;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;
import org.testcontainers.containers.PostgreSQLContainer;

import io.codemine.postgresql.TextInTextOutR2dbcCodec;
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
 * Integration tests for {@link EnumCodec} against a real PostgreSQL instance.
 *
 * <p>The Docker container is shared via a {@code static} field (started once). The JDBC and R2DBC
 * connections are instance fields created in the constructor so that they are always available,
 * regardless of whether jqwik re-creates the test instance for a {@code @Property} method.
 */
class EnumCodecIT {

  // -----------------------------------------------------------------------
  // Test enum and codec
  // -----------------------------------------------------------------------
  enum Mood {
    HAPPY,
    SAD,
    NEUTRAL
  }

  private static final EnumCodec<Mood> MOOD_CODEC =
      new EnumCodec<>(
          "", "test_mood", Map.of(Mood.HAPPY, "happy", Mood.SAD, "sad", Mood.NEUTRAL, "neutral"));

  // -----------------------------------------------------------------------
  // Shared container (started once) + per-instance connections
  // -----------------------------------------------------------------------
  static final PostgreSQLContainer<?> CONTAINER;
  private static final AtomicBoolean DDL_DONE = new AtomicBoolean(false);

  static {
    CONTAINER = new PostgreSQLContainer<>("postgres:18");
    CONTAINER.start();
  }

  private final java.sql.Connection pgjdbcConn;
  private final Connection textInTextOutConn;

  EnumCodecIT() {
    try {
      var props = new java.util.Properties();
      props.setProperty("user", CONTAINER.getUsername());
      props.setProperty("password", CONTAINER.getPassword());
      props.setProperty("prepareThreshold", "0");
      pgjdbcConn = DriverManager.getConnection(CONTAINER.getJdbcUrl(), props);

      if (DDL_DONE.compareAndSet(false, true)) {
        try (var stmt = pgjdbcConn.createStatement()) {
          stmt.execute("CREATE TYPE test_mood AS ENUM ('happy', 'sad', 'neutral')");
        }
      }

      var moodR2dbcCodec = new TextInTextOutR2dbcCodec<>(MOOD_CODEC, Mood.class);
      textInTextOutConn =
          Mono.from(
                  new PostgresqlConnectionFactory(
                          PostgresqlConnectionConfiguration.builder()
                              .host(CONTAINER.getHost())
                              .port(CONTAINER.getMappedPort(5432))
                              .username(CONTAINER.getUsername())
                              .password(CONTAINER.getPassword())
                              .database(CONTAINER.getDatabaseName())
                              .codecRegistrar(
                                  (c, allocator, registry) -> {
                                    registry.addFirst(moodR2dbcCodec);
                                    return Mono.empty();
                                  })
                              .build())
                      .create())
              .block();
    } catch (Exception e) {
      throw new RuntimeException("EnumCodecIT setup failed", e);
    }
  }

  // -----------------------------------------------------------------------
  // Providers
  // -----------------------------------------------------------------------
  @Provide
  Arbitrary<Mood> moods() {
    return Arbitraries.randomValue(MOOD_CODEC::random);
  }

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------
  @Test
  void oidMatchesName() throws Exception {
    try (var ps = pgjdbcConn.prepareStatement("SELECT oid::int FROM pg_type WHERE typname = ?")) {
      ps.setString(1, MOOD_CODEC.name());
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next(), "enum type test_mood not found in pg_type");
      }
    }
  }

  @Property(tries = 50)
  void roundtripsInTextToTextViaR2dbc(@ForAll("moods") Mood value) {
    Mood decoded =
        Flux.from(
                textInTextOutConn.createStatement("SELECT $1::test_mood").bind(0, value).execute())
            .flatMap(result -> result.map((row, meta) -> row.get(0, Mood.class)))
            .single()
            .block();
    assertEquals(value, decoded);
  }

  @Property(tries = 50)
  void roundtripsInTextToTextViaPgjdbc(@ForAll("moods") Mood value) throws Exception {
    try (var ps = pgjdbcConn.prepareStatement("SELECT ?::test_mood")) {
      PGobject obj = new PGobject();
      obj.setType("test_mood");
      StringBuilder sb = new StringBuilder();
      MOOD_CODEC.write(sb, value);
      obj.setValue(sb.toString());
      ps.setObject(1, obj);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        Mood decoded = MOOD_CODEC.parse(rs.getString(1), 0).value;
        assertEquals(value, decoded);
      }
    }
  }
}
