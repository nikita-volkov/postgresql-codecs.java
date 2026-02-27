package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class JsonbCodecIT extends CodecITBase {

    @Test
    void jsonbRoundTrip() throws Exception {
        String json = "{\"key\": \"value\"}";
        String result = roundTrip(Codec.JSONB, "jsonb", json);
        // JSONB may reformat
        assertNotNull(result);
        assertTrue(result.contains("key"));
        assertTrue(result.contains("value"));
    }

    @Test
    void jsonbNull() throws Exception {
        assertNull(roundTrip(Codec.JSONB, "jsonb", null));
    }
}
