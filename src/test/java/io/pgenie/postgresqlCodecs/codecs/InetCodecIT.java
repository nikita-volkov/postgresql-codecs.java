package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class InetCodecIT extends CodecITBase {

    @Test
    void inetIPv4() throws Exception {
        assertEquals("192.168.1.1", roundTrip(Codec.INET, "inet", "192.168.1.1"));
    }

    @Test
    void inetIPv6() throws Exception {
        String result = roundTrip(Codec.INET, "inet", "::1");
        assertNotNull(result);
        assertTrue(result.equals("::1") || result.contains("::1"));
    }

    @Test
    void inetCIDR() throws Exception {
        assertEquals("192.168.1.0/24", roundTrip(Codec.INET, "inet", "192.168.1.0/24"));
    }

    @Test
    void inetNull() throws Exception {
        assertNull(roundTrip(Codec.INET, "inet", null));
    }


    @Test
    void inetOid() throws Exception {
        assertOid(Codec.INET, "inet");
    }

    @Test
    void inetIpv4Binary() throws Exception {
        assertBinaryRoundTrip(Codec.INET, "inet", "192.168.1.1/32");
        assertBinaryRoundTrip(Codec.INET, "inet", "10.0.0.0/8");
        assertBinaryRoundTrip(Codec.INET, "inet", "0.0.0.0/0");
    }

    @Test
    void inetIpv6Binary() throws Exception {
        assertBinaryRoundTrip(Codec.INET, "inet", "::1/128");
        assertBinaryRoundTrip(Codec.INET, "inet", "2001:db8::/32");
    }

}
