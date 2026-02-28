package io.pgenie.postgresqlcodecs.codecs;

import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TimeCodecIT extends CodecITBase {

    @Test
    void timeRoundTrip() throws Exception {
        assertEquals(LocalTime.of(14, 30, 45),
                roundTrip(Codec.TIME, LocalTime.of(14, 30, 45)));
    }

    @Test
    void timeWithMicros() throws Exception {
        var t = LocalTime.of(14, 30, 45, 123456000);
        assertEquals(t, roundTrip(Codec.TIME, t));
    }

    @Test
    void timeNull() throws Exception {
        assertNull(roundTrip(Codec.TIME, null));
    }


    @Test
    void timeOid() throws Exception {
        assertOid(Codec.TIME);
    }

    @Test
    void timeBinary() throws Exception {
        assertBinaryRoundTrip(Codec.TIME, "time", LocalTime.MIDNIGHT);
        assertBinaryRoundTrip(Codec.TIME, "time", LocalTime.NOON);
        assertBinaryRoundTrip(Codec.TIME, "time", LocalTime.of(13, 45, 30, 123456000));
        assertBinaryRoundTrip(Codec.TIME, "time", LocalTime.of(23, 59, 59, 999999000));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#times")
    void timePropertyRoundTrip(LocalTime value) throws Exception {
        assertEquals(value, roundTrip(Codec.TIME, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#times")
    void timePropertyBinaryRoundTrip(LocalTime value) throws Exception {
        assertBinaryRoundTrip(Codec.TIME, "time", value);
    }

}
