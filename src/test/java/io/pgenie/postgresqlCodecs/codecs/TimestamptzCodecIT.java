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


    @Test
    void timestamptzOid() throws Exception {
        assertOid(Codec.TIMESTAMPTZ, "timestamptz");
    }

    @Test
    void timestamptzBinary() throws Exception {
        // timestamptz stores only the UTC instant; values must be UTC for assertEquals to pass
        assertBinaryRoundTrip(Codec.TIMESTAMPTZ, "timestamptz",
                OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
        assertBinaryRoundTrip(Codec.TIMESTAMPTZ, "timestamptz",
                OffsetDateTime.of(2024, 6, 15, 9, 30, 45, 123456000, ZoneOffset.UTC));
    }

}
