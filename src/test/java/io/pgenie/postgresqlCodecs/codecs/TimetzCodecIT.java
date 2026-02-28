package io.pgenie.postgresqlCodecs.codecs;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TimetzCodecIT extends CodecITBase {

    @Test
    void timetzRoundTrip() throws Exception {
        var t = OffsetTime.of(14, 30, 45, 0, ZoneOffset.ofHours(3));
        assertEquals(t, roundTrip(Codec.TIMETZ, t));
    }

    @Test
    void timetzNull() throws Exception {
        assertNull(roundTrip(Codec.TIMETZ, null));
    }


    @Test
    void timetzOid() throws Exception {
        assertOid(Codec.TIMETZ);
    }

    @Test
    void timetzBinary() throws Exception {
        assertBinaryRoundTrip(Codec.TIMETZ, "timetz", LocalTime.of(12, 0, 0).atOffset(ZoneOffset.UTC));
        assertBinaryRoundTrip(Codec.TIMETZ, "timetz", LocalTime.of(9, 30, 0).atOffset(ZoneOffset.ofHours(5)));
        assertBinaryRoundTrip(Codec.TIMETZ, "timetz", LocalTime.of(18, 0, 0).atOffset(ZoneOffset.ofHours(-8)));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#timetzes")
    void timetzPropertyRoundTrip(OffsetTime value) throws Exception {
        assertEquals(value, roundTrip(Codec.TIMETZ, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#timetzes")
    void timetzPropertyBinaryRoundTrip(OffsetTime value) throws Exception {
        assertBinaryRoundTrip(Codec.TIMETZ, "timetz", value);
    }

}
