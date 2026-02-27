package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class TsvectorCodecIT extends CodecITBase {

    @Test
    void tsvectorRoundTrip() throws Exception {
        // Note: PostgreSQL normalizes tsvectors
        String text = roundTripText(Codec.TSVECTOR, "tsvector", "'hello' 'world'");
        assertNotNull(text);
        assertTrue(text.contains("hello") && text.contains("world"));
    }

    @Test
    void tsvectorNull() throws Exception {
        assertNull(roundTripText(Codec.TSVECTOR, "tsvector", null));
    }
}
