package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class MoneyCodecIT extends CodecITBase {

    @Test
    void moneyRoundTrip() throws Exception {
        String text = roundTripText(Codec.MONEY, "money", "$100.50");
        assertNotNull(text);
        assertTrue(text.contains("100.50"));
    }

    @Test
    void moneyNull() throws Exception {
        assertNull(roundTripText(Codec.MONEY, "money", null));
    }
}
