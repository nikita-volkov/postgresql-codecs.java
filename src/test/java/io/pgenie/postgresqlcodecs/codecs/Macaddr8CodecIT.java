package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.pgenie.postgresqlcodecs.types.Macaddr8;

import io.pgenie.postgresqlcodecs.types.Macaddr8;

public class Macaddr8CodecIT extends CodecITBase {

    private static final Macaddr8 MAC_08002B0102030405 = new Macaddr8(
            (byte)0x08, (byte)0x00, (byte)0x2b, (byte)0x01,
            (byte)0x02, (byte)0x03, (byte)0x04, (byte)0x05);

    @Test
    void macaddr8RoundTrip() throws Exception {
        assertEquals(MAC_08002B0102030405, roundTrip(Codec.MACADDR8, MAC_08002B0102030405));
    }

    @Test
    void macaddr8Null() throws Exception {
        assertNull(roundTrip(Codec.MACADDR8, null));
    }

    @Test
    void macaddr8Oid() throws Exception {
        assertOid(Codec.MACADDR8);
    }

    @Test
    void macaddr8Binary() throws Exception {
        var mac = new Macaddr8(
                (byte)0x08, (byte)0x00, (byte)0x2b, (byte)0xff,
                (byte)0xfe, (byte)0x01, (byte)0x02, (byte)0x03);
        assertBinaryRoundTrip(Codec.MACADDR8, "macaddr8", mac);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#macaddr8s")
    void macaddr8PropertyRoundTrip(Macaddr8 value) throws Exception {
        assertEquals(value, roundTrip(Codec.MACADDR8, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#macaddr8s")
    void macaddr8PropertyBinaryRoundTrip(Macaddr8 value) throws Exception {
        assertBinaryRoundTrip(Codec.MACADDR8, "macaddr8", value);
    }

}
