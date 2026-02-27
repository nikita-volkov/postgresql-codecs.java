package io.pgenie.postgresqlCodecs.codecs;

import org.junit.jupiter.api.Test;

public class LineCodecIT extends CodecITBase {

    @Test
    void lineOid() throws Exception {
        assertOid(Codec.LINE, "line");
    }
}
