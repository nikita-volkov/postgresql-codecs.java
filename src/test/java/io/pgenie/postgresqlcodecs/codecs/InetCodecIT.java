package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.pgenie.postgresqlcodecs.types.Inet;

import io.pgenie.postgresqlcodecs.types.Inet;

public class InetCodecIT extends CodecITBase {

    // 192.168.1.1/32 as int: 0xC0A80101
    private static final Inet INET_IPV4_HOST =
            new Inet.V4(0xC0A80101, (byte) 32);

    // 192.168.1.0/24 as int: 0xC0A80100
    private static final Inet INET_IPV4_SUBNET =
            new Inet.V4(0xC0A80100, (byte) 24);

    // ::1/128
    private static final Inet INET_IPV6_LOOPBACK =
            new Inet.V6(0, 0, 0, 1, (byte) 128);

    // 2001:db8::/32 → w1=0x20010db8, w2=0, w3=0, w4=0, mask=32
    private static final Inet INET_IPV6_DOC =
            new Inet.V6(0x20010db8, 0, 0, 0, (byte) 32);

    @Test
    void inetIPv4() throws Exception {
        assertEquals(INET_IPV4_HOST, roundTrip(Codec.INET, INET_IPV4_HOST));
    }

    @Test
    void inetIPv4Subnet() throws Exception {
        assertEquals(INET_IPV4_SUBNET, roundTrip(Codec.INET, INET_IPV4_SUBNET));
    }

    @Test
    void inetIPv6() throws Exception {
        assertEquals(INET_IPV6_LOOPBACK, roundTrip(Codec.INET, INET_IPV6_LOOPBACK));
    }

    @Test
    void inetNull() throws Exception {
        assertNull(roundTrip(Codec.INET, null));
    }

    @Test
    void inetOid() throws Exception {
        assertOid(Codec.INET);
    }

    @Test
    void inetIpv4Binary() throws Exception {
        assertBinaryRoundTrip(Codec.INET, "inet", INET_IPV4_HOST);
        assertBinaryRoundTrip(Codec.INET, "inet", INET_IPV4_SUBNET);
        assertBinaryRoundTrip(Codec.INET, "inet", new Inet.V4(0, (byte) 0));
    }

    @Test
    void inetIpv6Binary() throws Exception {
        assertBinaryRoundTrip(Codec.INET, "inet", INET_IPV6_LOOPBACK);
        assertBinaryRoundTrip(Codec.INET, "inet", INET_IPV6_DOC);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#inets")
    void inetPropertyRoundTrip(Inet value) throws Exception {
        assertEquals(value, roundTrip(Codec.INET, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#inets")
    void inetPropertyBinaryRoundTrip(Inet value) throws Exception {
        assertBinaryRoundTrip(Codec.INET, "inet", value);
    }

}
