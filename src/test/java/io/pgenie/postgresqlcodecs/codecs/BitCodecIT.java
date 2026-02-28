package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.pgenie.postgresqlcodecs.types.Bit;

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

    /**
     * Property: arbitrary bit values round-trip through the binary codec.
     *
     * <p>Binary round-trip uses {@code bit(N)} with the exact bit count so that
     * PostgreSQL does not truncate or pad the value.
     */
    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#bits")
    void bitPropertyBinaryRoundTrip(Bit value) throws Exception {
        assertBinaryRoundTrip(Codec.BIT, "bit(" + value.numBits + ")", value);
    }

}
