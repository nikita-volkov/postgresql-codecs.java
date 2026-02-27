package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class CidrCodecIT extends CodecITBase {

    @Test
    void cidrRoundTrip() throws Exception {
        assertEquals("192.168.1.0/24", roundTrip(Codec.CIDR, "cidr", "192.168.1.0/24"));
    }

    @Test
    void cidrNull() throws Exception {
        assertNull(roundTrip(Codec.CIDR, "cidr", null));
    }
}
