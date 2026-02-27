package io.pgenie.postgresqlCodecs.codecs;

import java.time.OffsetTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class TimetzCodecIT extends CodecITBase {

    @Test
    void timetzRoundTrip() throws Exception {
        var t = OffsetTime.of(14, 30, 45, 0, ZoneOffset.ofHours(3));
        assertEquals(t, roundTrip(Codec.TIMETZ, "timetz", t));
    }

    @Test
    void timetzNull() throws Exception {
        assertNull(roundTrip(Codec.TIMETZ, "timetz", null));
    }
}
