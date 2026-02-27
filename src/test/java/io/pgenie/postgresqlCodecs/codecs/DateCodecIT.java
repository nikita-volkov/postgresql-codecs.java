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


    @Test
    void dateOid() throws Exception {
        assertOid(Codec.DATE, "date");
    }

    @Test
    void dateBinary() throws Exception {
        assertBinaryRoundTrip(Codec.DATE, "date", LocalDate.of(2000, 1, 1));
        assertBinaryRoundTrip(Codec.DATE, "date", LocalDate.of(1970, 1, 1));
        assertBinaryRoundTrip(Codec.DATE, "date", LocalDate.of(2024, 12, 31));
        assertBinaryRoundTrip(Codec.DATE, "date", LocalDate.of(1900, 6, 15));
    }

}
