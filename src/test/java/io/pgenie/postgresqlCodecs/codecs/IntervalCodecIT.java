package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class IntervalCodecIT extends CodecITBase {

    @Test
    void intervalRoundTrip() throws Exception {
        // PostgreSQL normalizes intervals; "1 year 2 mons 3 days" is canonical
        String text = roundTripText(Codec.INTERVAL, "interval", "1 year 2 mons 3 days");
        assertNotNull(text);
        assertTrue(text.contains("1 year"));
    }

    @Test
    void intervalNull() throws Exception {
        assertNull(roundTripText(Codec.INTERVAL, "interval", null));
    }
}
