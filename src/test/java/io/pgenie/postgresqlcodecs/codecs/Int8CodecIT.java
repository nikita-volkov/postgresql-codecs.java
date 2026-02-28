package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class Int8CodecIT extends CodecITBase {

    @Test
    void int8RoundTrip() throws Exception {
        assertEquals(9876543210L, roundTrip(Codec.INT8, 9876543210L));
    }

    @Test
    void int8Negative() throws Exception {
        assertEquals(-9876543210L, roundTrip(Codec.INT8, -9876543210L));
    }

    @Test
    void int8Null() throws Exception {
        assertNull(roundTrip(Codec.INT8, null));
    }


    @Test
    void int8Oid() throws Exception {
        assertOid(Codec.INT8);
    }

    @Test
    void int8Binary() throws Exception {
        assertBinaryRoundTrip(Codec.INT8, "int8", 0L);
        assertBinaryRoundTrip(Codec.INT8, "int8", 9876543210L);
        assertBinaryRoundTrip(Codec.INT8, "int8", -9876543210L);
        assertBinaryRoundTrip(Codec.INT8, "int8", Long.MAX_VALUE);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#int8s")
    void int8PropertyRoundTrip(Long value) throws Exception {
        assertEquals(value, roundTrip(Codec.INT8, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#int8s")
    void int8PropertyBinaryRoundTrip(Long value) throws Exception {
        assertBinaryRoundTrip(Codec.INT8, "int8", value);
    }

}
