package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class JsonCodecIT extends CodecITBase {

    @Test
    void jsonRoundTrip() throws Exception {
        String json = "{\"key\":\"value\",\"num\":42}";
        String result = roundTrip(Codec.JSON, json);
        // JSON preserves exact format
        assertEquals(json, result);
    }

    @Test
    void jsonNull() throws Exception {
        assertNull(roundTrip(Codec.JSON, null));
    }


    @Test
    void jsonOid() throws Exception {
        assertOid(Codec.JSON);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#jsons")
    void jsonPropertyRoundTrip(String value) throws Exception {
        // PostgreSQL json type preserves the exact text representation.
        assertEquals(value, roundTrip(Codec.JSON, value));
    }

}
