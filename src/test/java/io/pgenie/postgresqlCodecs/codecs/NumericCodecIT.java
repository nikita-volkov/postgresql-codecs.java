package io.pgenie.postgresqlCodecs.codecs;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class NumericCodecIT extends CodecITBase {

    @Test
    void numericRoundTrip() throws Exception {
        assertEquals(0, new BigDecimal("123456.789012").compareTo(
                roundTrip(Codec.NUMERIC, new BigDecimal("123456.789012"))));
    }

    @Test
    void numericZero() throws Exception {
        assertEquals(0, BigDecimal.ZERO.compareTo(
                roundTrip(Codec.NUMERIC, BigDecimal.ZERO)));
    }

    @Test
    void numericNegative() throws Exception {
        assertEquals(0, new BigDecimal("-99999.99").compareTo(
                roundTrip(Codec.NUMERIC, new BigDecimal("-99999.99"))));
    }

    @Test
    void numericNull() throws Exception {
        assertNull(roundTrip(Codec.NUMERIC, null));
    }


    @Test
    void numericOid() throws Exception {
        assertOid(Codec.NUMERIC);
    }

    @Test
    void numericBinary() throws Exception {
        assertBinaryRoundTrip(Codec.NUMERIC, "numeric", java.math.BigDecimal.ZERO);
        assertBinaryRoundTrip(Codec.NUMERIC, "numeric", new java.math.BigDecimal("1"));
        assertBinaryRoundTrip(Codec.NUMERIC, "numeric", new java.math.BigDecimal("123456.789"));
        assertBinaryRoundTrip(Codec.NUMERIC, "numeric", new java.math.BigDecimal("-0.00001"));
        assertBinaryRoundTrip(Codec.NUMERIC, "numeric", new java.math.BigDecimal("99999999999.99"));
        assertBinaryRoundTrip(Codec.NUMERIC, "numeric", new java.math.BigDecimal("0.1"));
    }

    /**
     * Property: arbitrary numeric values round-trip through the binary codec.
     *
     * <p>Uses {@code compareTo} because PostgreSQL may normalize trailing zeros
     * in the decimal scale, so {@code 1.10} and {@code 1.1} are numerically
     * equal even if not {@code equals}.
     */
    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#numerics")
    void numericPropertyBinaryRoundTrip(BigDecimal value) throws Exception {
        assertBinaryRoundTrip(Codec.NUMERIC, "numeric", value);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#numerics")
    void numericPropertyRoundTrip(BigDecimal value) throws Exception {
        BigDecimal result = roundTrip(Codec.NUMERIC, value);
        assertEquals(0, value.compareTo(result),
                "numeric round-trip mismatch: " + value + " != " + result);
    }

}
