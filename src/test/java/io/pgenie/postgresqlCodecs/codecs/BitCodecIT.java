package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class BitCodecIT extends CodecITBase {

    @Test
    void bitRoundTrip() throws Exception {
        assertEquals("10110", roundTrip(Codec.BIT, "bit(5)", "10110"));
    }

    @Test
    void bitNull() throws Exception {
        assertNull(roundTrip(Codec.BIT, "bit(5)", null));
    }


    @Test
    void bitOid() throws Exception {
        assertOid(Codec.BIT, "bit");
    }

    @Test
    void bitBinary() throws Exception {
        // pgType must match exact bit length
        String value = "101011";
        byte[] pgBytes = pgBinaryBytes(Codec.BIT, "bit(6)", value);
        assertEquals(hex(pgBytes), hex(Codec.BIT.encode(value)));
        assertEquals(value, Codec.BIT.decodeBinary(wrap(pgBytes), pgBytes.length));
    }

}
