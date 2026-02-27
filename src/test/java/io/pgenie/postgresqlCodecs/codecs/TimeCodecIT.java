package io.pgenie.postgresqlCodecs.codecs;

import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class TimeCodecIT extends CodecITBase {

    @Test
    void timeRoundTrip() throws Exception {
        assertEquals(LocalTime.of(14, 30, 45),
                roundTrip(Codec.TIME, "time", LocalTime.of(14, 30, 45)));
    }

    @Test
    void timeWithMicros() throws Exception {
        var t = LocalTime.of(14, 30, 45, 123456000);
        assertEquals(t, roundTrip(Codec.TIME, "time", t));
    }

    @Test
    void timeNull() throws Exception {
        assertNull(roundTrip(Codec.TIME, "time", null));
    }


    @Test
    void timeOid() throws Exception {
        assertOid(Codec.TIME, "time");
    }

    @Test
    void timeBinary() throws Exception {
        assertBinaryRoundTrip(Codec.TIME, "time", LocalTime.MIDNIGHT);
        assertBinaryRoundTrip(Codec.TIME, "time", LocalTime.NOON);
        assertBinaryRoundTrip(Codec.TIME, "time", LocalTime.of(13, 45, 30, 123456000));
        assertBinaryRoundTrip(Codec.TIME, "time", LocalTime.of(23, 59, 59, 999999000));
    }

}
