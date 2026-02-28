package io.pgenie.postgresqlcodecs.codecs;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CompositeCodecIT extends CodecITBase {

    @Test
    void compositeRoundTrip() throws Exception {
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    DO $$ BEGIN
                        CREATE TYPE test_comp AS (a int4, b text, c bool);
                    EXCEPTION WHEN duplicate_object THEN null;
                    END $$;
                    """);
        }
        record TestComp(Integer a, String b, Boolean c) {}
        var codec = new CompositeCodec<TestComp>(
                "public", "test_comp",
                (Integer a) -> (String b) -> (Boolean c) -> new TestComp(a, b, c),
                new CompositeCodec.Field<>("a", TestComp::a, Codec.INT4),
                new CompositeCodec.Field<>("b", TestComp::b, Codec.TEXT),
                new CompositeCodec.Field<>("c", TestComp::c, Codec.BOOL));

        var input = new TestComp(42, "hello world", true);
        try (var ps = conn.prepareStatement("SELECT ?::test_comp")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input.a(), result.value.a());
                assertEquals(input.b(), result.value.b());
                assertEquals(input.c(), result.value.c());
            }
        }
    }

    @Test
    void compositeWithNullFields() throws Exception {
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    DO $$ BEGIN
                        CREATE TYPE test_comp2 AS (a int4, b text);
                    EXCEPTION WHEN duplicate_object THEN null;
                    END $$;
                    """);
        }
        record TestComp2(Integer a, String b) {}
        var codec = new CompositeCodec<TestComp2>(
                "public", "test_comp2",
                (Integer a) -> (String b) -> new TestComp2(a, b),
                new CompositeCodec.Field<>("a", TestComp2::a, Codec.INT4),
                new CompositeCodec.Field<>("b", TestComp2::b, Codec.TEXT));

        var input = new TestComp2(null, "hello");
        try (var ps = conn.prepareStatement("SELECT ?::test_comp2")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertNull(result.value.a());
                assertEquals("hello", result.value.b());
            }
        }
    }

    @Test
    void compositeWithSpecialChars() throws Exception {
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    DO $$ BEGIN
                        CREATE TYPE test_comp3 AS (a text, b text);
                    EXCEPTION WHEN duplicate_object THEN null;
                    END $$;
                    """);
        }
        record TestComp3(String a, String b) {}
        var codec = new CompositeCodec<TestComp3>(
                "public", "test_comp3",
                (String a) -> (String b) -> new TestComp3(a, b),
                new CompositeCodec.Field<>("a", TestComp3::a, Codec.TEXT),
                new CompositeCodec.Field<>("b", TestComp3::b, Codec.TEXT));

        var input = new TestComp3("hello, world", "she said \"hi\"");
        try (var ps = conn.prepareStatement("SELECT ?::test_comp3")) {
            codec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = codec.parse(text, 0);
                assertEquals(input.a(), result.value.a());
                assertEquals(input.b(), result.value.b());
            }
        }
    }

    @Test
    void compositeNull() throws Exception {
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    DO $$ BEGIN
                        CREATE TYPE test_comp4 AS (a int4, b text);
                    EXCEPTION WHEN duplicate_object THEN null;
                    END $$;
                    """);
        }
        record TestComp4(Integer a, String b) {}
        var codec = new CompositeCodec<TestComp4>(
                "public", "test_comp4",
                (Integer a) -> (String b) -> new TestComp4(a, b),
                new CompositeCodec.Field<>("a", TestComp4::a, Codec.INT4),
                new CompositeCodec.Field<>("b", TestComp4::b, Codec.TEXT));

        try (var ps = conn.prepareStatement("SELECT ?::test_comp4")) {
            codec.bind(ps, 1, null);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
            }
        }
    }

    @Test
    void compositeWriteAsRow() throws Exception {
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    DO $$ BEGIN
                        CREATE TYPE test_row_comp AS (x int4, y text);
                    EXCEPTION WHEN duplicate_object THEN null;
                    END $$;
                    """);
        }
        record RowComp(Integer x, String y) {}
        var codec = new CompositeCodec<RowComp>(
                "public", "test_row_comp",
                (Integer x) -> (String y) -> new RowComp(x, y),
                new CompositeCodec.Field<>("x", RowComp::x, Codec.INT4),
                new CompositeCodec.Field<>("y", RowComp::y, Codec.TEXT));

        var input = new RowComp(42, "hello");
        var sb = new StringBuilder();
        codec.writeAsRow(sb, input);
        String rowExpr = sb.toString();
        assertTrue(rowExpr.startsWith("row("));
        assertTrue(rowExpr.endsWith(")"));

        // Verify it works as SQL by executing it
        try (var stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT (" + rowExpr + "::test_row_comp).*")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                assertEquals("hello", rs.getString(2));
            }
        }
    }

    @Test
    void arrayOfCompositesRoundTrip() throws Exception {
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    DO $$ BEGIN
                        CREATE TYPE test_arr_comp AS (id int4, name text);
                    EXCEPTION WHEN duplicate_object THEN null;
                    END $$;
                    """);
        }
        record ArrComp(Integer id, String name) {}
        var elemCodec = new CompositeCodec<ArrComp>(
                "public", "test_arr_comp",
                (Integer id) -> (String name) -> new ArrComp(id, name),
                new CompositeCodec.Field<>("id", ArrComp::id, Codec.INT4),
                new CompositeCodec.Field<>("name", ArrComp::name, Codec.TEXT));
        var arrayCodec = new ArrayCodec<>("_test_arr_comp", elemCodec);

        var input = List.of(new ArrComp(1, "Alice"), new ArrComp(2, "Bob"));
        try (var ps = conn.prepareStatement("SELECT ?::test_arr_comp[]")) {
            arrayCodec.bind(ps, 1, input);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String text = rs.getString(1);
                assertNotNull(text);
                var result = arrayCodec.parse(text, 0);
                assertEquals(2, result.value.size());
                assertEquals(1, result.value.get(0).id());
                assertEquals("Alice", result.value.get(0).name());
                assertEquals(2, result.value.get(1).id());
                assertEquals("Bob", result.value.get(1).name());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Write then parse unit tests (no database)
    // -----------------------------------------------------------------------

    @Test
    void compositeWriteParseRoundTripNoDB() throws Exception {
        record Pair(Integer x, String y) {}
        var codec = new CompositeCodec<Pair>(
                "public", "pair",
                (Integer x) -> (String y) -> new Pair(x, y),
                new CompositeCodec.Field<>("x", Pair::x, Codec.INT4),
                new CompositeCodec.Field<>("y", Pair::y, Codec.TEXT));
        var input = new Pair(42, "test value");
        var sb = new StringBuilder();
        codec.write(sb, input);
        var result = codec.parse(sb.toString(), 0);
        assertEquals(input.x(), result.value.x());
        assertEquals(input.y(), result.value.y());
    }

    @Test
    void compositeWriteParseWithNullFieldsNoDB() throws Exception {
        record Pair(Integer x, String y) {}
        var codec = new CompositeCodec<Pair>(
                "public", "pair",
                (Integer x) -> (String y) -> new Pair(x, y),
                new CompositeCodec.Field<>("x", Pair::x, Codec.INT4),
                new CompositeCodec.Field<>("y", Pair::y, Codec.TEXT));
        var input = new Pair(null, null);
        var sb = new StringBuilder();
        codec.write(sb, input);
        var result = codec.parse(sb.toString(), 0);
        assertNull(result.value.x());
        assertNull(result.value.y());
    }


    @Test
    void compositeBinaryRoundTrip() throws Exception {
        record BinPoint(int x, int y) {}
        var codec = new CompositeCodec<>("public", "mypoint",
                (Integer x) -> (Integer y) -> new BinPoint(x, y),
                new CompositeCodec.Field<>("x", BinPoint::x, Codec.INT4),
                new CompositeCodec.Field<>("y", BinPoint::y, Codec.INT4));

        var value = new BinPoint(7, -3);
        byte[] encoded = codec.encode(value);
        assertEquals(value, codec.decodeBinary(wrap(encoded), encoded.length));
    }

    @Test
    void compositeWithNullFieldBinaryRoundTrip() throws Exception {
        record BinNamed(String name, Integer count) {}
        var codec = new CompositeCodec<>("public", "named",
                (String name) -> (Integer count) -> new BinNamed(name, count),
                new CompositeCodec.Field<>("name", BinNamed::name, Codec.TEXT),
                new CompositeCodec.Field<>("count", BinNamed::count, Codec.INT4));

        var value = new BinNamed("alice", null);
        byte[] encoded = codec.encode(value);
        assertEquals(value, codec.decodeBinary(wrap(encoded), encoded.length));
    }

}
