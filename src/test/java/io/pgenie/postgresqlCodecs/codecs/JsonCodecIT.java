package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class JsonCodecIT extends CodecITBase {

    @Test
    void jsonRoundTrip() throws Exception {
        String json = "{\"key\":\"value\",\"num\":42}";
        String result = roundTrip(Codec.JSON, "json", json);
        // JSON preserves exact format
        assertEquals(json, result);
    }

    @Test
    void jsonNull() throws Exception {
        assertNull(roundTrip(Codec.JSON, "json", null));
    }
}
