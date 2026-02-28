package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class Int2CodecIT extends CodecITBase {

    @Test
    void int2RoundTrip() throws Exception {
        assertEquals((short) 12345, roundTrip(Codec.INT2, (short) 12345));
    }

    @Test
    void int2Negative() throws Exception {
        assertEquals((short) -32000, roundTrip(Codec.INT2, (short) -32000));
    }

    @Test
    void int2Null() throws Exception {
        assertNull(roundTrip(Codec.INT2, null));
    }


    @Test
    void int2Oid() throws Exception {
        assertOid(Codec.INT2);
    }

    @Test
    void int2Binary() throws Exception {
        assertBinaryRoundTrip(Codec.INT2, "int2", (short) 0);
        assertBinaryRoundTrip(Codec.INT2, "int2", (short) 32767);
        assertBinaryRoundTrip(Codec.INT2, "int2", (short) -32768);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#int2s")
    void int2PropertyRoundTrip(Short value) throws Exception {
        assertEquals(value, roundTrip(Codec.INT2, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#int2s")
    void int2PropertyBinaryRoundTrip(Short value) throws Exception {
        assertBinaryRoundTrip(Codec.INT2, "int2", value);
    }

}
