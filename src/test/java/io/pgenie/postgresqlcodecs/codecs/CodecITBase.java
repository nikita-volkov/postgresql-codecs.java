package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.postgresql.util.PGobject;
import org.testcontainers.containers.PostgreSQLContainer;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Base class for codec integration tests. Provides a shared PostgreSQL
 * container and a single reused JDBC connection for all tests.
 *
 * <p>
 * Both the container and the connection are started once per JVM via a static
 * initializer and cleaned up automatically by the TestContainers resource
 * reaper (Ryuk) on JVM exit.
 *
 * <p>
 * Reusing a single connection across tests avoids the overhead of TCP handshake
 * and PostgreSQL session setup on every round-trip, which is the dominant cost
 * when running many property-based tests.
 */
abstract class CodecITBase {

    static final PostgreSQLContainer<?> pg;
    static final Connection conn;
    static final PostgresqlConnectionFactory r2dbcFactory;

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
        r2dbcFactory = new PostgresqlConnectionFactory(
                PostgresqlConnectionConfiguration.builder()
                        .host(pg.getHost())
                        .port(pg.getMappedPort(5432))
                        .username(pg.getUsername())
                        .password(pg.getPassword())
                        .database(pg.getDatabaseName())
                        // Force binary wire format for all result columns so that
                        // RawBinaryCodec receives the exact binary representation.
                        .forceBinary(true)
                        .codecRegistrar((c, allocator, registry) -> {
                            registry.addFirst(new RawBinaryCodec());
                            return Mono.empty();
                        })
                        .build());
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

    // -----------------------------------------------------------------------
    // Binary helpers
    // -----------------------------------------------------------------------
    /**
     * Returns the binary wire bytes PostgreSQL uses for {@code value} by
     * executing {@code SELECT $1::typeSig} via r2dbc-postgresql.
     *
     * <p>
     * r2dbc-postgresql uses the binary extended-query protocol natively. With
     * {@code forceBinary} enabled, every result column is transmitted in binary
     * wire format. {@link RawBinaryCodec} intercepts the column before any
     * type-specific decoding occurs and copies the raw bytes out of the Netty
     * {@code ByteBuf}, giving us the exact binary representation without any
     * intermediate SQL function call.
     */
    <A> byte[] pgBinaryBytes(Codec<A> codec, A value) throws Exception {
        var sb = new StringBuilder();
        codec.write(sb, value);
        return Mono.usingWhen(
                r2dbcFactory.create(),
                r2conn -> Flux.from(
                        r2conn.createStatement("SELECT $1::" + codec.typeSig())
                                .bind(0, sb.toString())
                                .execute()
                ).flatMap(result -> result.map((row, meta) -> row.get(0, byte[].class)))
                        .single(),
                c -> c.close()
        ).block();
    }

    /**
     * Passes raw binary bytes from a result column directly through to the
     * caller, bypassing all type-specific codec logic in r2dbc-postgresql.
     *
     * <p>
     * {@link #canDecode} returns {@code true} for any PostgreSQL OID in
     * {@code FORMAT_BINARY}, ensuring that the driver requests binary result
     * format and routes the column data here. The bytes are copied once from
     * the Netty buffer and returned as a plain {@code byte[]}.
     */
    private static class RawBinaryCodec
            implements io.r2dbc.postgresql.codec.Codec<byte[]> {

        @Override
        public boolean canDecode(int dataType,
                io.r2dbc.postgresql.message.Format format,
                Class<?> type) {
            return format == io.r2dbc.postgresql.message.Format.FORMAT_BINARY
                    && type.isAssignableFrom(byte[].class);
        }

        @Override
        public boolean canEncode(Object value) {
            return false;
        }

        @Override
        public boolean canEncodeNull(Class<?> type) {
            return false;
        }

        @Override
        public byte[] decode(io.netty.buffer.ByteBuf buffer,
                int dataType,
                io.r2dbc.postgresql.message.Format format,
                Class<? extends byte[]> type) {
            if (buffer == null) {
                return null;
            }
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            return bytes;
        }

        @Override
        public io.r2dbc.postgresql.client.EncodedParameter encode(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.r2dbc.postgresql.client.EncodedParameter encode(Object value, int dataType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.r2dbc.postgresql.client.EncodedParameter encodeNull() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Asserts that {@code codec.encode(value)} matches PostgreSQL's binary
     * representation, and that {@code codec.decodeBinary} recovers the original
     * value via {@link Object#equals}.
     */
    <A> void assertBinaryRoundTrip(Codec<A> codec, A value) throws Exception {

        byte[] pgBytes = pgBinaryBytes(codec, value);
        byte[] ourBytes = codec.encode(value);

        String pgHex = HexFormat.of().formatHex(pgBytes);
        String ourHex = HexFormat.of().formatHex(ourBytes);

        assertEquals(pgHex, ourHex,
                "encode mismatch for " + codec.typeSig() + " value=" + value);

        A decoded = codec.decodeBinary(
                ByteBuffer.wrap(pgBytes).order(ByteOrder.BIG_ENDIAN),
                pgBytes.length);

        assertEquals(value, decoded,
                "decode mismatch for " + codec.typeSig() + " value=" + value);

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
