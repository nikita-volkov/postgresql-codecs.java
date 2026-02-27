package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class ByteaCodecIT extends CodecITBase {

    @Test
    void byteaRoundTrip() throws Exception {
        byte[] input = new byte[]{0x01, 0x02, (byte) 0xFF, 0x00, 0x7F};
        String text = roundTripText(Codec.BYTEA, "bytea", input);
        assertNotNull(text);
        var parsed = Codec.BYTEA.parse(text, 0);
        assertArrayEquals(input, parsed.value);
    }

    @Test
    void byteaEmpty() throws Exception {
        byte[] input = new byte[0];
        String text = roundTripText(Codec.BYTEA, "bytea", input);
        assertNotNull(text);
        var parsed = Codec.BYTEA.parse(text, 0);
        assertArrayEquals(input, parsed.value);
    }

    @Test
    void byteaNull() throws Exception {
        assertNull(roundTripText(Codec.BYTEA, "bytea", null));
    }
}
