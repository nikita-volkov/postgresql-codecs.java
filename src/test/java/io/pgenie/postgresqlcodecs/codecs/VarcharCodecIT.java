package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class VarcharCodecIT extends CodecITBase {

    @Test
    void varcharRoundTrip() throws Exception {
        assertEquals("hello", roundTrip(Codec.VARCHAR, "hello"));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#varchars")
    void varcharPropertyRoundTrip(String value) throws Exception {
        assertEquals(value, roundTrip(Codec.VARCHAR, value));
    }

}
