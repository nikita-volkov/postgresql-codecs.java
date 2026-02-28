package io.pgenie.postgresqlCodecs.codecs;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TimestampCodecIT extends CodecITBase {

    @Test
    void timestampRoundTrip() throws Exception {
        var ts = LocalDateTime.of(2024, 6, 15, 14, 30, 45);
        assertEquals(ts, roundTrip(Codec.TIMESTAMP, ts));
    }

    @Test
    void timestampWithMicros() throws Exception {
        var ts = LocalDateTime.of(2024, 6, 15, 14, 30, 45, 123456000);
        assertEquals(ts, roundTrip(Codec.TIMESTAMP, ts));
    }

    @Test
    void timestampNull() throws Exception {
        assertNull(roundTrip(Codec.TIMESTAMP, null));
    }


    @Test
    void timestampOid() throws Exception {
        assertOid(Codec.TIMESTAMP);
    }

    @Test
    void timestampBinary() throws Exception {
        assertBinaryRoundTrip(Codec.TIMESTAMP, "timestamp", LocalDateTime.of(2000, 1, 1, 0, 0, 0));
        assertBinaryRoundTrip(Codec.TIMESTAMP, "timestamp", LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123456000));
        assertBinaryRoundTrip(Codec.TIMESTAMP, "timestamp", LocalDateTime.of(1970, 1, 1, 0, 0, 0));
    }

    /**
     * Property: arbitrary timestamps spanning PostgreSQL's full range (4713 BC to 294276 AD)
     * round-trip through both the text and binary codecs.
     *
     * <p>Text round-trip uses AD-only timestamps to avoid JDBC BC-date binding limitations;
     * binary round-trip uses the full range.
     */
    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#timestampsAD")
    void timestampPropertyRoundTrip(LocalDateTime value) throws Exception {
        assertEquals(value, roundTrip(Codec.TIMESTAMP, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#timestamps")
    void timestampPropertyBinaryRoundTrip(LocalDateTime value) throws Exception {
        assertBinaryRoundTrip(Codec.TIMESTAMP, "timestamp", value);
    }

}
