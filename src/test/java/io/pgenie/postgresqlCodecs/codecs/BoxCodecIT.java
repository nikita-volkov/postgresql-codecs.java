package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

public class BoxCodecIT extends CodecITBase {

    @Test
    void boxRoundTrip() throws Exception {
        var box = new org.postgresql.geometric.PGbox(3.0, 4.0, 1.0, 2.0);
        var result = roundTrip(Codec.BOX, "box", box);
        assertNotNull(result);
    }


    @Test
    void boxOid() throws Exception {
        assertOid(Codec.BOX, "box");
    }

    @Test
    void boxBinary() throws Exception {
        assertBinaryRoundTrip(Codec.BOX, "box", new org.postgresql.geometric.PGbox(2, 2, 0, 0));
    }

}
