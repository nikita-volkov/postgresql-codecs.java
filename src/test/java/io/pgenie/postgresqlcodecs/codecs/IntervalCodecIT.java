package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.pgenie.postgresqlcodecs.types.Interval;

import io.pgenie.postgresqlcodecs.types.Interval;

public class IntervalCodecIT extends CodecITBase {

    @Test
    void intervalRoundTripYearMonthDay() throws Exception {
        // 1 year 2 months 3 days = 14 months, 3 days, 0 micros
        var interval = new Interval(14, 3, 0);
        assertEquals(interval, roundTrip(Codec.INTERVAL, interval));
    }

    @Test
    void intervalRoundTripTime() throws Exception {
        // 4 hours 5 minutes 6 seconds = 14706 seconds = 14706000000 microseconds
        var interval = new Interval(0, 0, 14_706_000_000L);
        assertEquals(interval, roundTrip(Codec.INTERVAL, interval));
    }

    @Test
    void intervalRoundTripNegative() throws Exception {
        var interval = new Interval(-6, -1, -3_600_000_000L);
        assertEquals(interval, roundTrip(Codec.INTERVAL, interval));
    }

    @Test
    void intervalNull() throws Exception {
        assertNull(roundTrip(Codec.INTERVAL, null));
    }

    @Test
    void intervalOid() throws Exception {
        assertOid(Codec.INTERVAL);
    }

    @Test
    void intervalBinary() throws Exception {
        assertBinaryRoundTrip(Codec.INTERVAL, "interval", new Interval(14, 3, 14_706_000_000L));
        assertBinaryRoundTrip(Codec.INTERVAL, "interval", new Interval(0, 0, 0));
        assertBinaryRoundTrip(Codec.INTERVAL, "interval", new Interval(-12, 0, -1_000_000L));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#intervals")
    void intervalPropertyBinaryRoundTrip(Interval value) throws Exception {
        assertBinaryRoundTrip(Codec.INTERVAL, "interval", value);
    }

}
