package io.pgenie.postgresqlCodecs.codecs;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class UuidCodecIT extends CodecITBase {

    @Test
    void uuidRoundTrip() throws Exception {
        UUID id = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        assertEquals(id, roundTrip(Codec.UUID, id));
    }

    @Test
    void uuidRandom() throws Exception {
        UUID id = UUID.randomUUID();
        assertEquals(id, roundTrip(Codec.UUID, id));
    }

    @Test
    void uuidNull() throws Exception {
        assertNull(roundTrip(Codec.UUID, null));
    }


    @Test
    void uuidOid() throws Exception {
        assertOid(Codec.UUID);
    }

    @Test
    void uuidBinary() throws Exception {
        assertBinaryRoundTrip(Codec.UUID, "uuid", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertBinaryRoundTrip(Codec.UUID, "uuid", UUID.randomUUID());
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#uuids")
    void uuidPropertyRoundTrip(UUID value) throws Exception {
        assertEquals(value, roundTrip(Codec.UUID, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#uuids")
    void uuidPropertyBinaryRoundTrip(UUID value) throws Exception {
        assertBinaryRoundTrip(Codec.UUID, "uuid", value);
    }

}
