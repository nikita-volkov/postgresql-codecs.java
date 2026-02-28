package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.pgenie.postgresqlCodecs.types.Cidr;

import io.pgenie.postgresqlCodecs.types.Cidr;

public class CidrCodecIT extends CodecITBase {

    // 192.168.1.0/24
    private static final Cidr CIDR_IPV4 = new Cidr.V4(0xC0A80100, (byte) 24);

    // ::/0
    private static final Cidr CIDR_IPV6_DEFAULT = new Cidr.V6(0, 0, 0, 0, (byte) 0);

    @Test
    void cidrRoundTrip() throws Exception {
        assertEquals(CIDR_IPV4, roundTrip(Codec.CIDR, CIDR_IPV4));
    }

    @Test
    void cidrNull() throws Exception {
        assertNull(roundTrip(Codec.CIDR, null));
    }

    @Test
    void cidrOid() throws Exception {
        assertOid(Codec.CIDR);
    }

    @Test
    void cidrIpv4Binary() throws Exception {
        assertBinaryRoundTrip(Codec.CIDR, "cidr", CIDR_IPV4);
        assertBinaryRoundTrip(Codec.CIDR, "cidr", new Cidr.V4(0x0A000000, (byte) 8));
    }

    @Test
    void cidrIpv6Binary() throws Exception {
        assertBinaryRoundTrip(Codec.CIDR, "cidr", CIDR_IPV6_DEFAULT);
        assertBinaryRoundTrip(Codec.CIDR, "cidr", new Cidr.V6(0x20010db8, 0, 0, 0, (byte) 32));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#cidrs")
    void cidrPropertyRoundTrip(Cidr value) throws Exception {
        assertEquals(value, roundTrip(Codec.CIDR, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#cidrs")
    void cidrPropertyBinaryRoundTrip(Cidr value) throws Exception {
        assertBinaryRoundTrip(Codec.CIDR, "cidr", value);
    }

}
