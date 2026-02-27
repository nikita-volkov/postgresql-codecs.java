package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class Float8CodecIT extends CodecITBase {

    @Test
    void float8RoundTrip() throws Exception {
        assertEquals(3.141592653589793, roundTrip(Codec.FLOAT8, "float8", 3.141592653589793));
    }

    @Test
    void float8NaN() throws Exception {
        assertTrue(Double.isNaN(roundTrip(Codec.FLOAT8, "float8", Double.NaN)));
    }

    @Test
    void float8Infinity() throws Exception {
        assertEquals(Double.POSITIVE_INFINITY, roundTrip(Codec.FLOAT8, "float8", Double.POSITIVE_INFINITY));
    }

    @Test
    void float8Null() throws Exception {
        assertNull(roundTrip(Codec.FLOAT8, "float8", null));
    }
}
