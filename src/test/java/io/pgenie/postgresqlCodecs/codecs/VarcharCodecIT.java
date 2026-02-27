package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class VarcharCodecIT extends CodecITBase {

    @Test
    void varcharRoundTrip() throws Exception {
        assertEquals("hello", roundTrip(Codec.VARCHAR, "varchar", "hello"));
    }
}
