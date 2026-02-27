package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class CharCodecIT extends CodecITBase {

    @Test
    void charRoundTrip() throws Exception {
        // char(5) pads with spaces
        String result = roundTrip(Codec.CHAR, "char(5)", "ab");
        assertEquals("ab   ", result);
    }
}
