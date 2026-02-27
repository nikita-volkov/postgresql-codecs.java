package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class TextCodecIT extends CodecITBase {

    @Test
    void textRoundTrip() throws Exception {
        assertEquals("Hello, World!", roundTrip(Codec.TEXT, "text", "Hello, World!"));
    }

    @Test
    void textEmpty() throws Exception {
        assertEquals("", roundTrip(Codec.TEXT, "text", ""));
    }

    @Test
    void textSpecialChars() throws Exception {
        assertEquals("It's a \"test\" with \\backslash",
                roundTrip(Codec.TEXT, "text", "It's a \"test\" with \\backslash"));
    }

    @Test
    void textNull() throws Exception {
        assertNull(roundTrip(Codec.TEXT, "text", null));
    }
}
