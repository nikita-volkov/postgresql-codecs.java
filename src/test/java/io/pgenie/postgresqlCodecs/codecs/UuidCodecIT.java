package io.pgenie.postgresqlCodecs.codecs;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class UuidCodecIT extends CodecITBase {

    @Test
    void uuidRoundTrip() throws Exception {
        UUID id = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        assertEquals(id, roundTrip(Codec.UUID, "uuid", id));
    }

    @Test
    void uuidRandom() throws Exception {
        UUID id = UUID.randomUUID();
        assertEquals(id, roundTrip(Codec.UUID, "uuid", id));
    }

    @Test
    void uuidNull() throws Exception {
        assertNull(roundTrip(Codec.UUID, "uuid", null));
    }
}
