package io.pgenie.postgresqlcodecs.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base class for codec integration tests. Provides a shared PostgreSQL
 * container and a single reused JDBC connection for all tests.
 *
 * <p>Both the container and the connection are started once per JVM via a
 * static initializer and cleaned up automatically by the TestContainers
 * resource reaper (Ryuk) on JVM exit.
 *
 * <p>Reusing a single connection across tests avoids the overhead of TCP
 * handshake and PostgreSQL session setup on every round-trip, which is the
 * dominant cost when running many property-based tests.
 */
abstract class CodecITBase {

    static final PostgreSQLContainer<?> pg;
    static final Connection conn;

    static {
        pg = new PostgreSQLContainer<>("postgres:18");
        pg.start();
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
            throw new RuntimeException("Failed to open shared connection", e);
        }
    }

    /**
     * Generic helper: binds a value using the codec, sends it through
     * PostgreSQL via a cast expression, reads back the text representation, and
     * parses it.
     */
    <A> A roundTrip(Codec<A> codec, A value) throws Exception {
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

            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Expected a result row");
                String text = rs.getString(1);
                if (text == null) {
                    return null;
                }
                var result = codec.parse(text, 0);
                return result.value;
            }
        }
    }

    /**
     * Helper for types where we just check string equality of results (useful
     * when the Java type doesn't have a natural equals, like byte[]).
     */
    <A> String roundTripText(Codec<A> codec, String castType, A value) throws Exception {
        try (var ps = conn.prepareStatement("SELECT ?::" + castType)) {
            codec.bind(ps, 1, value);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Expected a result row");
                return rs.getString(1);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Binary helpers
    // -----------------------------------------------------------------------
    /**
     * Returns the binary wire bytes PostgreSQL uses for {@code value} of type
     * {@code pgType}, obtained by inserting the value into a temporary table
     * and reading it back via {@code COPY TO STDOUT BINARY}.
     *
     * <p>
     * This avoids any {@code *send()} SQL functions in the query text.
     */
    <A> byte[] pgBinaryBytes(Codec<A> codec, String pgType, A value) throws Exception {
        try (var s = conn.createStatement()) {
            // DROP + CREATE instead of CREATE TEMP to handle connection reuse.
            s.execute("DROP TABLE IF EXISTS _bce");
            s.execute("CREATE TEMP TABLE _bce (v " + pgType + ")");
        }
        try (var ps = conn.prepareStatement("INSERT INTO _bce VALUES (?)")) {
            codec.bind(ps, 1, value);
            ps.execute();
        }
        var cm = new CopyManager(conn.unwrap(BaseConnection.class));
        var baos = new ByteArrayOutputStream();
        cm.copyOut("COPY _bce TO STDOUT BINARY", baos);
        // COPY binary layout:
        //   11-byte signature + 4-byte flags + 4-byte header_ext_len = 19 bytes
        //   then per row: int16 field_count, int32 field_len, byte[field_len] data
        var buf = ByteBuffer.wrap(baos.toByteArray()).order(ByteOrder.BIG_ENDIAN);
        buf.position(19);        // skip file header
        buf.getShort();          // field count (1)
        int len = buf.getInt();  // field data length
        byte[] result = new byte[len];
        buf.get(result);
        return result;
    }

    /**
     * Hex-encodes a byte array.
     */
    static String hex(byte[] b) {
        return HexFormat.of().formatHex(b);
    }

    /**
     * Wraps {@code b} in a big-endian {@link ByteBuffer} ready for decoding.
     */
    static ByteBuffer wrap(byte[] b) {
        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
    }

    /**
     * Asserts that {@code codec.encode(value)} matches PostgreSQL's binary
     * representation, and that {@code codec.decodeBinary} recovers the original
     * value via {@link Object#equals}.
     */
    <A> void assertBinaryRoundTrip(Codec<A> codec, String pgType, A value) throws Exception {
        byte[] pgBytes = pgBinaryBytes(codec, pgType, value);
        byte[] ourBytes = codec.encode(value);
        assertEquals(hex(pgBytes), hex(ourBytes),
                "encode mismatch for " + codec.name() + " value=" + value);
        A decoded = codec.decodeBinary(wrap(pgBytes), pgBytes.length);
        assertEquals(value, decoded,
                "decode mismatch for " + codec.name() + " value=" + value);
    }

    /**
     * Asserts that {@code codec.oid()} matches the OID stored in
     * {@code pg_type}.
     */
    void assertOid(Codec<?> codec) throws Exception {
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
}
