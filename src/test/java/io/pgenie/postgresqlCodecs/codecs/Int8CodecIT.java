package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class Int8CodecIT extends CodecITBase {

    @Test
    void int8RoundTrip() throws Exception {
        assertEquals(9876543210L, roundTrip(Codec.INT8, "int8", 9876543210L));
    }

    @Test
    void int8Negative() throws Exception {
        assertEquals(-9876543210L, roundTrip(Codec.INT8, "int8", -9876543210L));
    }

    @Test
    void int8Null() throws Exception {
        assertNull(roundTrip(Codec.INT8, "int8", null));
    }
}
