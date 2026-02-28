package io.pgenie.postgresqlcodecs.codecs;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TimestamptzCodecIT extends CodecITBase {

    @Test
    void timestamptzRoundTrip() throws Exception {
        var ts = OffsetDateTime.of(2024, 6, 15, 14, 30, 45, 0, ZoneOffset.UTC);
        var result = roundTrip(Codec.TIMESTAMPTZ, ts);
        assertEquals(ts.toInstant(), result.toInstant());
    }

    @Test
    void timestamptzNull() throws Exception {
        assertNull(roundTrip(Codec.TIMESTAMPTZ, null));
    }


    @Test
    void timestamptzOid() throws Exception {
        assertOid(Codec.TIMESTAMPTZ);
    }

    @Test
    void timestamptzBinary() throws Exception {
        // timestamptz stores only the UTC instant; values must be UTC for assertEquals to pass
        assertBinaryRoundTrip(Codec.TIMESTAMPTZ, "timestamptz",
                OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
        assertBinaryRoundTrip(Codec.TIMESTAMPTZ, "timestamptz",
                OffsetDateTime.of(2024, 6, 15, 9, 30, 45, 123456000, ZoneOffset.UTC));
    }

    /**
     * Property: arbitrary UTC timestamptz values spanning PostgreSQL's full range
     * round-trip through both text and binary codecs.
     *
     * <p>Values are UTC-normalised because PostgreSQL stores {@code timestamptz}
     * as a UTC instant; the binary decoder returns a UTC value, so equality
     * comparisons are only meaningful for UTC input.
     *
     * <p>Text round-trip uses AD-only timestamps to avoid JDBC BC-date binding limitations;
     * binary round-trip uses the full range.
     */
    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#timestamptzADs")
    void timestamptzPropertyRoundTrip(OffsetDateTime value) throws Exception {
        assertEquals(value.toInstant(), roundTrip(Codec.TIMESTAMPTZ, value).toInstant());
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#timestamptzs")
    void timestamptzPropertyBinaryRoundTrip(OffsetDateTime value) throws Exception {
        assertBinaryRoundTrip(Codec.TIMESTAMPTZ, "timestamptz", value);
    }

}
