package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGlseg;

public class LsegCodecIT extends CodecITBase {

    @Test
    void lsegRoundTrip() throws Exception {
        var lseg = new PGlseg(1.0, 2.0, 3.0, 4.0);
        var result = roundTrip(Codec.LSEG, lseg);
        assertNotNull(result);
    }


    @Test
    void lsegOid() throws Exception {
        assertOid(Codec.LSEG);
    }

    @Test
    void lsegBinary() throws Exception {
        assertBinaryRoundTrip(Codec.LSEG, "lseg", new PGlseg(0, 0, 1, 1));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#lsegs")
    void lsegPropertyBinaryRoundTrip(PGlseg value) throws Exception {
        assertBinaryRoundTrip(Codec.LSEG, "lseg", value);
    }

}
