package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class VarbitCodecIT extends CodecITBase {

    @Test
    void varbitRoundTrip() throws Exception {
        assertEquals("1011010", roundTrip(Codec.VARBIT, "varbit", "1011010"));
    }

    @Test
    void varbitNull() throws Exception {
        assertNull(roundTrip(Codec.VARBIT, "varbit", null));
    }


    @Test
    void varbitOid() throws Exception {
        assertOid(Codec.VARBIT, "varbit");
    }

    @Test
    void varbitBinary() throws Exception {
        String value = "10110";
        byte[] pgBytes = pgBinaryBytes(Codec.VARBIT, "varbit", value);
        assertEquals(hex(pgBytes), hex(Codec.VARBIT.encode(value)));
        assertEquals(value, Codec.VARBIT.decodeBinary(wrap(pgBytes), pgBytes.length));
    }

}
