package io.pgenie.postgresqlCodecs.codecs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base class for codec integration tests. Provides a shared PostgreSQL container
 * and helper methods for round-trip testing.
 *
 * The container is started once per JVM via a static initializer and cleaned up
 * automatically by the TestContainers resource reaper (Ryuk) on JVM exit.
 */
abstract class CodecITBase {

    static final PostgreSQLContainer<?> pg;

    static {
        pg = new PostgreSQLContainer<>("postgres:18");
        pg.start();
    }

    Connection connect() throws SQLException {
        return DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
    }

    /**
     * Generic helper: binds a value using the codec, sends it through PostgreSQL
     * via a cast expression, reads back the text representation, and parses it.
     */
    <A> A roundTrip(Codec<A> codec, String castType, A value) throws Exception {
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::" + castType)) {
            codec.bind(ps, 1, value);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Expected a result row");
                String text = rs.getString(1);
                if (text == null) return null;
                var result = codec.parse(text, 0);
                return result.value;
            }
        }
    }

    /**
     * Helper for types where we just check string equality of results
     * (useful when the Java type doesn't have a natural equals, like byte[]).
     */
    <A> String roundTripText(Codec<A> codec, String castType, A value) throws Exception {
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::" + castType)) {
            codec.bind(ps, 1, value);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Expected a result row");
                return rs.getString(1);
            }
        }
    }
}
