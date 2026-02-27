package io.pgenie.postgresqlCodecs.codecs;

import java.time.LocalTime;
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


    @Test
    void timetzOid() throws Exception {
        assertOid(Codec.TIMETZ, "timetz");
    }

    @Test
    void timetzBinary() throws Exception {
        assertBinaryRoundTrip(Codec.TIMETZ, "timetz", LocalTime.of(12, 0, 0).atOffset(ZoneOffset.UTC));
        assertBinaryRoundTrip(Codec.TIMETZ, "timetz", LocalTime.of(9, 30, 0).atOffset(ZoneOffset.ofHours(5)));
        assertBinaryRoundTrip(Codec.TIMETZ, "timetz", LocalTime.of(18, 0, 0).atOffset(ZoneOffset.ofHours(-8)));
    }

}
