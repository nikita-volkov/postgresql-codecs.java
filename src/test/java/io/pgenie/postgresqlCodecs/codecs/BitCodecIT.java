package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class BitCodecIT extends CodecITBase {

    @Test
    void bitRoundTrip() throws Exception {
        var bit = Bit.fromBitString("1");
        assertEquals(bit, roundTrip(Codec.BIT, bit));
    }

    @Test
    void bitNull() throws Exception {
        assertNull(roundTrip(Codec.BIT, null));
    }

    @Test
    void bitOid() throws Exception {
        assertOid(Codec.BIT);
    }

    @Test
    void bitBinary() throws Exception {
        var value = Bit.fromBitString("101011");
        byte[] pgBytes = pgBinaryBytes(Codec.BIT, "bit(6)", value);
        assertEquals(hex(pgBytes), hex(Codec.BIT.encode(value)));
        assertEquals(value, Codec.BIT.decodeBinary(wrap(pgBytes), pgBytes.length));
    }

}
