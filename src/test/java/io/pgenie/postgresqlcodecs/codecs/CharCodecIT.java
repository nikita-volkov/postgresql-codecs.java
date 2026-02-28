package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CharCodecIT extends CodecITBase {

    @Test
    void charRoundTrip() throws Exception {
        assertEquals("a", roundTrip(Codec.CHAR, "a"));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#chars")
    void charPropertyRoundTrip(String value) throws Exception {
        // PostgreSQL char(1) blank-pads, so trimming is needed for comparison.
        String result = roundTrip(Codec.CHAR, value);
        assertEquals(value, result == null ? null : result.stripTrailing());
    }

}
