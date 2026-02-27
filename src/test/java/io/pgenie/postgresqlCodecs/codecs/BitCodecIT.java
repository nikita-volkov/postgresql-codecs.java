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
}
