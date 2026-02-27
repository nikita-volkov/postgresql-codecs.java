package io.pgenie.postgresqlCodecs.codecs;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class TimestampCodecIT extends CodecITBase {

    @Test
    void timestampRoundTrip() throws Exception {
        var ts = LocalDateTime.of(2024, 6, 15, 14, 30, 45);
        assertEquals(ts, roundTrip(Codec.TIMESTAMP, "timestamp", ts));
    }

    @Test
    void timestampWithMicros() throws Exception {
        var ts = LocalDateTime.of(2024, 6, 15, 14, 30, 45, 123456000);
        assertEquals(ts, roundTrip(Codec.TIMESTAMP, "timestamp", ts));
    }

    @Test
    void timestampNull() throws Exception {
        assertNull(roundTrip(Codec.TIMESTAMP, "timestamp", null));
    }
}
