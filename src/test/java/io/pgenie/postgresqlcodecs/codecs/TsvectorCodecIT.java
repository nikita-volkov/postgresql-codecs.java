package io.pgenie.postgresqlcodecs.codecs;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.pgenie.postgresqlcodecs.types.Tsvector;

public class TsvectorCodecIT extends CodecITBase {

    private static Tsvector simpleTs() {
        return Tsvector.of(List.of(
                Map.entry("hello", List.of()),
                Map.entry("world", List.of())));
    }

    @Test
    void tsvectorRoundTrip() throws Exception {
        Tsvector ts = simpleTs();
        assertEquals(ts, roundTrip(Codec.TSVECTOR, ts));
    }

    @Test
    void tsvectorWithPositions() throws Exception {
        var ts = Tsvector.of(List.of(
                Map.entry("cat", List.of(
                        new Tsvector.Position((short) 1, Tsvector.Weight.A),
                        new Tsvector.Position((short) 3, Tsvector.Weight.D))),
                Map.entry("dog", List.of(
                        new Tsvector.Position((short) 2, Tsvector.Weight.B)))));
        assertEquals(ts, roundTrip(Codec.TSVECTOR, ts));
    }

    @Test
    void tsvectorNull() throws Exception {
        assertNull(roundTrip(Codec.TSVECTOR, null));
    }

    @Test
    void tsvectorOid() throws Exception {
        assertOid(Codec.TSVECTOR);
    }

    @Test
    void tsvectorBinary() throws Exception {
        assertBinaryRoundTrip(Codec.TSVECTOR, "tsvector", simpleTs());
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#tsvectors")
    void tsvectorPropertyRoundTrip(Tsvector value) throws Exception {
        assertEquals(value, roundTrip(Codec.TSVECTOR, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#tsvectors")
    void tsvectorPropertyBinaryRoundTrip(Tsvector value) throws Exception {
        assertBinaryRoundTrip(Codec.TSVECTOR, "tsvector", value);
    }

}
