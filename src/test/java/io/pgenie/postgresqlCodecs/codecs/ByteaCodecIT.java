package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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


    @Test
    void byteaOid() throws Exception {
        assertOid(Codec.BYTEA);
    }

    @Test
    void byteaBinary() throws Exception {
        byte[] value = new byte[]{0x00, 0x01, (byte) 0xFF, (byte) 0xAB, 0x42};
        byte[] pgBytes = pgBinaryBytes(Codec.BYTEA, "bytea", value);
        assertArrayEquals(pgBytes, Codec.BYTEA.encode(value));
        assertArrayEquals(value, Codec.BYTEA.decodeBinary(wrap(pgBytes), pgBytes.length));
    }

    @Test
    void byteaEmptyBinary() throws Exception {
        byte[] value = new byte[0];
        byte[] pgBytes = pgBinaryBytes(Codec.BYTEA, "bytea", value);
        assertArrayEquals(pgBytes, Codec.BYTEA.encode(value));
        assertArrayEquals(value, Codec.BYTEA.decodeBinary(wrap(pgBytes), pgBytes.length));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#byteas")
    void byteaPropertyRoundTrip(byte[] value) throws Exception {
        String text = roundTripText(Codec.BYTEA, "bytea", value);
        assertNotNull(text);
        assertArrayEquals(value, Codec.BYTEA.parse(text, 0).value);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#byteas")
    void byteaPropertyBinaryRoundTrip(byte[] value) throws Exception {
        byte[] pgBytes = pgBinaryBytes(Codec.BYTEA, "bytea", value);
        assertArrayEquals(pgBytes, Codec.BYTEA.encode(value));
        assertArrayEquals(value, Codec.BYTEA.decodeBinary(wrap(pgBytes), pgBytes.length));
    }

}
