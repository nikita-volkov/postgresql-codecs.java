package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class Float4CodecIT extends CodecITBase {

    @Test
    void float4RoundTrip() throws Exception {
        assertEquals(3.14f, roundTrip(Codec.FLOAT4, "float4", 3.14f), 0.001f);
    }

    @Test
    void float4NaN() throws Exception {
        assertTrue(Float.isNaN(roundTrip(Codec.FLOAT4, "float4", Float.NaN)));
    }

    @Test
    void float4Infinity() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, roundTrip(Codec.FLOAT4, "float4", Float.POSITIVE_INFINITY));
    }

    @Test
    void float4NegInfinity() throws Exception {
        assertEquals(Float.NEGATIVE_INFINITY, roundTrip(Codec.FLOAT4, "float4", Float.NEGATIVE_INFINITY));
    }

    @Test
    void float4Null() throws Exception {
        assertNull(roundTrip(Codec.FLOAT4, "float4", null));
    }
}
