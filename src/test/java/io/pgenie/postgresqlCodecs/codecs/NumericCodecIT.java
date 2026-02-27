package io.pgenie.postgresqlCodecs.codecs;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class NumericCodecIT extends CodecITBase {

    @Test
    void numericRoundTrip() throws Exception {
        assertEquals(0, new BigDecimal("123456.789012").compareTo(
                roundTrip(Codec.NUMERIC, "numeric", new BigDecimal("123456.789012"))));
    }

    @Test
    void numericZero() throws Exception {
        assertEquals(0, BigDecimal.ZERO.compareTo(
                roundTrip(Codec.NUMERIC, "numeric", BigDecimal.ZERO)));
    }

    @Test
    void numericNegative() throws Exception {
        assertEquals(0, new BigDecimal("-99999.99").compareTo(
                roundTrip(Codec.NUMERIC, "numeric", new BigDecimal("-99999.99"))));
    }

    @Test
    void numericNull() throws Exception {
        assertNull(roundTrip(Codec.NUMERIC, "numeric", null));
    }
}
