package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.pgenie.postgresqlcodecs.types.Macaddr;

import io.pgenie.postgresqlcodecs.types.Macaddr;

public class MacaddrCodecIT extends CodecITBase {

    private static final Macaddr MAC_08002B010203 =
            new Macaddr((byte)0x08, (byte)0x00, (byte)0x2b, (byte)0x01, (byte)0x02, (byte)0x03);

    @Test
    void macaddrRoundTrip() throws Exception {
        assertEquals(MAC_08002B010203, roundTrip(Codec.MACADDR, MAC_08002B010203));
    }

    @Test
    void macaddrNull() throws Exception {
        assertNull(roundTrip(Codec.MACADDR, null));
    }

    @Test
    void macaddrOid() throws Exception {
        assertOid(Codec.MACADDR);
    }

    @Test
    void macaddrBinary() throws Exception {
        assertBinaryRoundTrip(Codec.MACADDR, "macaddr", MAC_08002B010203);
        assertBinaryRoundTrip(Codec.MACADDR, "macaddr",
                new Macaddr((byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#macaddrs")
    void macaddrPropertyRoundTrip(Macaddr value) throws Exception {
        assertEquals(value, roundTrip(Codec.MACADDR, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#macaddrs")
    void macaddrPropertyBinaryRoundTrip(Macaddr value) throws Exception {
        assertBinaryRoundTrip(Codec.MACADDR, "macaddr", value);
    }

}
