package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class BoolCodecIT extends CodecITBase {

    @Test
    void boolTrue() throws Exception {
        assertEquals(true, roundTrip(Codec.BOOL, true));
    }

    @Test
    void boolFalse() throws Exception {
        assertEquals(false, roundTrip(Codec.BOOL, false));
    }

    @Test
    void boolNull() throws Exception {
        assertNull(roundTrip(Codec.BOOL, null));
    }


    @Test
    void boolOid() throws Exception {
        assertOid(Codec.BOOL);
    }

    @Test
    void boolTrueBinary() throws Exception {
        assertBinaryRoundTrip(Codec.BOOL, "bool", true);
    }

    @Test
    void boolFalseBinary() throws Exception {
        assertBinaryRoundTrip(Codec.BOOL, "bool", false);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#booleans")
    void boolPropertyRoundTrip(Boolean value) throws Exception {
        assertEquals(value, roundTrip(Codec.BOOL, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#booleans")
    void boolPropertyBinaryRoundTrip(Boolean value) throws Exception {
        assertBinaryRoundTrip(Codec.BOOL, "bool", value);
    }

}
