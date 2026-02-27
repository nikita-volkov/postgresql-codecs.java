package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class VarbitCodecIT extends CodecITBase {

    @Test
    void varbitRoundTrip() throws Exception {
        assertEquals("1011010", roundTrip(Codec.VARBIT, "varbit", "1011010"));
    }

    @Test
    void varbitNull() throws Exception {
        assertNull(roundTrip(Codec.VARBIT, "varbit", null));
    }
}
