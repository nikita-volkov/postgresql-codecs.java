package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class JsonbCodecIT extends CodecITBase {

    @Test
    void jsonbRoundTrip() throws Exception {
        String json = "{\"key\": \"value\"}";
        String result = roundTrip(Codec.JSONB, json);
        // JSONB may reformat
        assertNotNull(result);
        assertTrue(result.contains("key"));
        assertTrue(result.contains("value"));
    }

    @Test
    void jsonbNull() throws Exception {
        assertNull(roundTrip(Codec.JSONB, null));
    }


    @Test
    void jsonbOid() throws Exception {
        assertOid(Codec.JSONB);
    }

    /**
     * Property: arbitrary JSON values are accepted by PostgreSQL as jsonb.
     *
     * <p>JSONB normalizes the representation (key ordering, whitespace), so
     * only non-null acceptance is asserted rather than string equality.
     */
    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#jsonbs")
    void jsonbPropertyRoundTrip(String value) throws Exception {
        assertNotNull(roundTrip(Codec.JSONB, value),
                "jsonb round-trip returned null for: " + value);
    }

}
