package io.pgenie.postgresqlCodecs.codecs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base class for codec integration tests. Provides a shared PostgreSQL container
 * and helper methods for round-trip testing.
 */
abstract class CodecITBase {

    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:18");

    @BeforeAll
    static void startContainer() {
        pg.start();
    }

    @AfterAll
    static void stopContainer() {
        pg.stop();
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
