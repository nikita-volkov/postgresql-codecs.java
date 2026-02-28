package io.pgenie.postgresqlCodecs.codecs;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ArrayCodecIT extends CodecITBase {

    // -----------------------------------------------------------------------
    // Array of Int4
    // -----------------------------------------------------------------------

    @Test
    void int4ArrayRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_int4", Codec.INT4);
        var input = List.of(1, 2, 3);
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::int4[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input, result.value);
            }
        }
    }

    @Test
    void int4ArrayEmpty() throws Exception {
        var codec = new ArrayCodec<>("_int4", Codec.INT4);
        List<Integer> input = List.of();
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::int4[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input, result.value);
            }
        }
    }

    @Test
    void int4ArrayWithNulls() throws Exception {
        var codec = new ArrayCodec<>("_int4", Codec.INT4);
        var input = new java.util.ArrayList<Integer>();
        input.add(1);
        input.add(null);
        input.add(3);
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::int4[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(3, result.value.size());
                assertEquals(1, result.value.get(0));
                assertNull(result.value.get(1));
                assertEquals(3, result.value.get(2));
            }
        }
    }

    @Test
    void int4ArrayNull() throws Exception {
        var codec = new ArrayCodec<>("_int4", Codec.INT4);
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::int4[]")) {
            codec.bind(ps, 1, null);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Array of Text
    // -----------------------------------------------------------------------

    @Test
    void textArrayRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_text", Codec.TEXT);
        var input = List.of("hello", "world", "foo bar");
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::text[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input, result.value);
            }
        }
    }

    @Test
    void textArrayWithSpecialChars() throws Exception {
        var codec = new ArrayCodec<>("_text", Codec.TEXT);
        var input = List.of("a,b", "c\"d", "e\\f", "");
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::text[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input, result.value);
            }
        }
    }

    @Test
    void textArrayWithNulls() throws Exception {
        var codec = new ArrayCodec<>("_text", Codec.TEXT);
        var input = new java.util.ArrayList<String>();
        input.add("hello");
        input.add(null);
        input.add("world");
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::text[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(3, result.value.size());
                assertEquals("hello", result.value.get(0));
                assertNull(result.value.get(1));
                assertEquals("world", result.value.get(2));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Array of Bool
    // -----------------------------------------------------------------------

    @Test
    void boolArrayRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_bool", Codec.BOOL);
        var input = List.of(true, false, true);
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::bool[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input, result.value);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Array of Float8
    // -----------------------------------------------------------------------

    @Test
    void float8ArrayRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_float8", Codec.FLOAT8);
        var input = List.of(1.1, 2.2, 3.3);
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::float8[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(3, result.value.size());
                assertEquals(1.1, result.value.get(0), 0.0001);
                assertEquals(2.2, result.value.get(1), 0.0001);
                assertEquals(3.3, result.value.get(2), 0.0001);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Array of UUID
    // -----------------------------------------------------------------------

    @Test
    void uuidArrayRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_uuid", Codec.UUID);
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        var input = List.of(id1, id2);
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::uuid[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input, result.value);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Array of Date
    // -----------------------------------------------------------------------

    @Test
    void dateArrayRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_date", Codec.DATE);
        var input = List.of(LocalDate.of(2024, 1, 1), LocalDate.of(2024, 12, 31));
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::date[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input, result.value);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Array of Timestamp
    // -----------------------------------------------------------------------

    @Test
    void timestampArrayRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_timestamp", Codec.TIMESTAMP);
        var ts1 = LocalDateTime.of(2024, 6, 15, 10, 30, 0);
        var ts2 = LocalDateTime.of(2024, 12, 25, 23, 59, 59);
        var input = List.of(ts1, ts2);
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::timestamp[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input, result.value);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Array of Inet (PGobject-based in arrays)
    // -----------------------------------------------------------------------

    @Test
    void inetArrayRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_inet", Codec.INET);
        // 192.168.1.1/32 = 0xC0A80101, 10.0.0.1/32 = 0x0A000001
        var input = List.<Inet>of(new Inet.V4(0xC0A80101, (byte) 32), new Inet.V4(0x0A000001, (byte) 32));
        try (var conn = connect();
             var ps = conn.prepareStatement("SELECT ?::inet[]")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input, result.value);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Write then parse unit tests (no database)
    // -----------------------------------------------------------------------

    @Test
    void arrayWriteParseRoundTripNoDB() throws Exception {
        var codec = new ArrayCodec<>("_int4", Codec.INT4);
        var input = List.of(10, 20, 30);
        var sb = new StringBuilder();
        codec.write(sb, input);
        var result = codec.parse(sb.toString(), 0);
        assertEquals(input, result.value);
    }

    @Test
    void arrayWriteParseWithNullsNoDB() throws Exception {
        var codec = new ArrayCodec<>("_text", Codec.TEXT);
        var input = new java.util.ArrayList<String>();
        input.add("a");
        input.add(null);
        input.add("c");
        var sb = new StringBuilder();
        codec.write(sb, input);
        var result = codec.parse(sb.toString(), 0);
        assertEquals(input, result.value);
    }


    @Test
    void arrayInt4BinaryRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_int4", Codec.INT4);
        var list = java.util.List.of(1, 2, 3, -100, Integer.MAX_VALUE);
        byte[] encoded = codec.encode(list);
        assertEquals(list, codec.decodeBinary(wrap(encoded), encoded.length));
    }

    @Test
    void arrayTextBinaryRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_text", Codec.TEXT);
        var list = java.util.List.of("hello", "world", "Unicode: \u4e2d\u6587");
        byte[] encoded = codec.encode(list);
        assertEquals(list, codec.decodeBinary(wrap(encoded), encoded.length));
    }

    @Test
    void arrayWithNullBinaryRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_int4", Codec.INT4);
        var list = new java.util.ArrayList<Integer>();
        list.add(1);
        list.add(null);
        list.add(3);
        byte[] encoded = codec.encode(list);
        assertEquals(list, codec.decodeBinary(wrap(encoded), encoded.length));
    }

    @Test
    void arrayEmptyBinaryRoundTrip() throws Exception {
        var codec = new ArrayCodec<>("_int4", Codec.INT4);
        var list = java.util.List.<Integer>of();
        byte[] encoded = codec.encode(list);
        assertEquals(list, codec.decodeBinary(wrap(encoded), encoded.length));
    }

}
