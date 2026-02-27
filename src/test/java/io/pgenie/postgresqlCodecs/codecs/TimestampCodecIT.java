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


    @Test
    void timestampOid() throws Exception {
        assertOid(Codec.TIMESTAMP, "timestamp");
    }

    @Test
    void timestampBinary() throws Exception {
        assertBinaryRoundTrip(Codec.TIMESTAMP, "timestamp", LocalDateTime.of(2000, 1, 1, 0, 0, 0));
        assertBinaryRoundTrip(Codec.TIMESTAMP, "timestamp", LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123456000));
        assertBinaryRoundTrip(Codec.TIMESTAMP, "timestamp", LocalDateTime.of(1970, 1, 1, 0, 0, 0));
    }

}
