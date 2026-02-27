package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class MacaddrCodecIT extends CodecITBase {

    @Test
    void macaddrRoundTrip() throws Exception {
        assertEquals("08:00:2b:01:02:03", roundTrip(Codec.MACADDR, "macaddr", "08:00:2b:01:02:03"));
    }

    @Test
    void macaddrNull() throws Exception {
        assertNull(roundTrip(Codec.MACADDR, "macaddr", null));
    }
}
