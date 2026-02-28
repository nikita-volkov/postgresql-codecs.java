package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class Float8CodecIT extends CodecITBase {

    @Test
    void float8RoundTrip() throws Exception {
        assertEquals(3.141592653589793, roundTrip(Codec.FLOAT8, 3.141592653589793));
    }

    @Test
    void float8NaN() throws Exception {
        assertTrue(Double.isNaN(roundTrip(Codec.FLOAT8, Double.NaN)));
    }

    @Test
    void float8Infinity() throws Exception {
        assertEquals(Double.POSITIVE_INFINITY, roundTrip(Codec.FLOAT8, Double.POSITIVE_INFINITY));
    }

    @Test
    void float8Null() throws Exception {
        assertNull(roundTrip(Codec.FLOAT8, null));
    }


    @Test
    void float8Oid() throws Exception {
        assertOid(Codec.FLOAT8);
    }

    @Test
    void float8Binary() throws Exception {
        assertBinaryRoundTrip(Codec.FLOAT8, "float8", 0.0);
        assertBinaryRoundTrip(Codec.FLOAT8, "float8", Math.PI);
        assertBinaryRoundTrip(Codec.FLOAT8, "float8", -1.23456789e10);
    }

    /** Property: finite float8 values round-trip through the binary codec exactly. */
    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#float8s")
    void float8PropertyBinaryRoundTrip(Double value) throws Exception {
        assertBinaryRoundTrip(Codec.FLOAT8, "float8", value);
    }

}
