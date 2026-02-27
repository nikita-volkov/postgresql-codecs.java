package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class Int2CodecIT extends CodecITBase {

    @Test
    void int2RoundTrip() throws Exception {
        assertEquals((short) 12345, roundTrip(Codec.INT2, "int2", (short) 12345));
    }

    @Test
    void int2Negative() throws Exception {
        assertEquals((short) -32000, roundTrip(Codec.INT2, "int2", (short) -32000));
    }

    @Test
    void int2Null() throws Exception {
        assertNull(roundTrip(Codec.INT2, "int2", null));
    }


    @Test
    void int2Oid() throws Exception {
        assertOid(Codec.INT2, "int2");
    }

    @Test
    void int2Binary() throws Exception {
        assertBinaryRoundTrip(Codec.INT2, "int2", (short) 0);
        assertBinaryRoundTrip(Codec.INT2, "int2", (short) 32767);
        assertBinaryRoundTrip(Codec.INT2, "int2", (short) -32768);
    }

}
