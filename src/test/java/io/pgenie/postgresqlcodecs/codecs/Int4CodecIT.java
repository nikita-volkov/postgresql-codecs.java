package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class Int4CodecIT extends CodecITBase {

    @Test
    void int4RoundTrip() throws Exception {
        assertEquals(42, roundTrip(Codec.INT4, 42));
    }

    @Test
    void int4Negative() throws Exception {
        assertEquals(-100000, roundTrip(Codec.INT4, -100000));
    }

    @Test
    void int4Null() throws Exception {
        assertNull(roundTrip(Codec.INT4, null));
    }


    @Test
    void int4Oid() throws Exception {
        assertOid(Codec.INT4);
    }

    @Test
    void int4Binary() throws Exception {
        assertBinaryRoundTrip(Codec.INT4, "int4", 0);
        assertBinaryRoundTrip(Codec.INT4, "int4", 42);
        assertBinaryRoundTrip(Codec.INT4, "int4", -1);
        assertBinaryRoundTrip(Codec.INT4, "int4", Integer.MAX_VALUE);
        assertBinaryRoundTrip(Codec.INT4, "int4", Integer.MIN_VALUE);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#int4s")
    void int4PropertyRoundTrip(Integer value) throws Exception {
        assertEquals(value, roundTrip(Codec.INT4, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#int4s")
    void int4PropertyBinaryRoundTrip(Integer value) throws Exception {
        assertBinaryRoundTrip(Codec.INT4, "int4", value);
    }

}
