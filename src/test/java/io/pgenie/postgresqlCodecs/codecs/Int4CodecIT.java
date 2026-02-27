package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class Int4CodecIT extends CodecITBase {

    @Test
    void int4RoundTrip() throws Exception {
        assertEquals(42, roundTrip(Codec.INT4, "int4", 42));
    }

    @Test
    void int4Negative() throws Exception {
        assertEquals(-100000, roundTrip(Codec.INT4, "int4", -100000));
    }

    @Test
    void int4Null() throws Exception {
        assertNull(roundTrip(Codec.INT4, "int4", null));
    }
}
