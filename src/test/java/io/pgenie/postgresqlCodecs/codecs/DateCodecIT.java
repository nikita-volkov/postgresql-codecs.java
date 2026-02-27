package io.pgenie.postgresqlCodecs.codecs;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class DateCodecIT extends CodecITBase {

    @Test
    void dateRoundTrip() throws Exception {
        assertEquals(LocalDate.of(2024, 6, 15),
                roundTrip(Codec.DATE, "date", LocalDate.of(2024, 6, 15)));
    }

    @Test
    void dateNull() throws Exception {
        assertNull(roundTrip(Codec.DATE, "date", null));
    }
}
