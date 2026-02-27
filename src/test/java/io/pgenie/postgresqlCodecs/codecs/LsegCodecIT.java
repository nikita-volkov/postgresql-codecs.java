package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

public class LsegCodecIT extends CodecITBase {

    @Test
    void lsegRoundTrip() throws Exception {
        var lseg = new org.postgresql.geometric.PGlseg(1.0, 2.0, 3.0, 4.0);
        var result = roundTrip(Codec.LSEG, "lseg", lseg);
        assertNotNull(result);
    }


    @Test
    void lsegOid() throws Exception {
        assertOid(Codec.LSEG, "lseg");
    }

    @Test
    void lsegBinary() throws Exception {
        assertBinaryRoundTrip(Codec.LSEG, "lseg", new org.postgresql.geometric.PGlseg(0, 0, 1, 1));
    }

}
