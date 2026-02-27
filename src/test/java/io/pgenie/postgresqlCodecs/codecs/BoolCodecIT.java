package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class BoolCodecIT extends CodecITBase {

    @Test
    void boolTrue() throws Exception {
        assertEquals(true, roundTrip(Codec.BOOL, "bool", true));
    }

    @Test
    void boolFalse() throws Exception {
        assertEquals(false, roundTrip(Codec.BOOL, "bool", false));
    }

    @Test
    void boolNull() throws Exception {
        assertNull(roundTrip(Codec.BOOL, "bool", null));
    }


    @Test
    void boolOid() throws Exception {
        assertOid(Codec.BOOL, "bool");
    }

    @Test
    void boolTrueBinary() throws Exception {
        assertBinaryRoundTrip(Codec.BOOL, "bool", true);
    }

    @Test
    void boolFalseBinary() throws Exception {
        assertBinaryRoundTrip(Codec.BOOL, "bool", false);
    }

}
