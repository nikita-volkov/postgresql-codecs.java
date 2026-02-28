package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TextCodecIT extends CodecITBase {

    @Test
    void textRoundTrip() throws Exception {
        assertEquals("Hello, World!", roundTrip(Codec.TEXT, "Hello, World!"));
    }

    @Test
    void textEmpty() throws Exception {
        assertEquals("", roundTrip(Codec.TEXT, ""));
    }

    @Test
    void textSpecialChars() throws Exception {
        assertEquals("It's a \"test\" with \\backslash",
                roundTrip(Codec.TEXT, "It's a \"test\" with \\backslash"));
    }

    @Test
    void textNull() throws Exception {
        assertNull(roundTrip(Codec.TEXT, null));
    }


    @Test
    void textOid() throws Exception {
        assertOid(Codec.TEXT);
    }

    @Test
    void textBinary() throws Exception {
        assertBinaryRoundTrip(Codec.TEXT, "text", "");
        assertBinaryRoundTrip(Codec.TEXT, "text", "hello");
        assertBinaryRoundTrip(Codec.TEXT, "text", "Unicode: \u00e9\u4e2d\u6587");
        assertBinaryRoundTrip(Codec.TEXT, "text", "line1\nline2");
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#texts")
    void textPropertyRoundTrip(String value) throws Exception {
        assertEquals(value, roundTrip(Codec.TEXT, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#texts")
    void textPropertyBinaryRoundTrip(String value) throws Exception {
        assertBinaryRoundTrip(Codec.TEXT, "text", value);
    }

}
