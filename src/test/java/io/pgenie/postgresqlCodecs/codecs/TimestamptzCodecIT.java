package io.pgenie.postgresqlCodecs.codecs;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class TimestamptzCodecIT extends CodecITBase {

    @Test
    void timestamptzRoundTrip() throws Exception {
        var ts = OffsetDateTime.of(2024, 6, 15, 14, 30, 45, 0, ZoneOffset.UTC);
        var result = roundTrip(Codec.TIMESTAMPTZ, "timestamptz", ts);
        assertEquals(ts.toInstant(), result.toInstant());
    }

    @Test
    void timestamptzNull() throws Exception {
        assertNull(roundTrip(Codec.TIMESTAMPTZ, "timestamptz", null));
    }
}
