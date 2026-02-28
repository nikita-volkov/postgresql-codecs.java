package io.pgenie.postgresqlcodecs.codecs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGline;

public class LineCodecIT extends CodecITBase {

    @Test
    void lineOid() throws Exception {
        assertOid(Codec.LINE);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#lines")
    void linePropertyBinaryRoundTrip(PGline value) throws Exception {
        assertBinaryRoundTrip(Codec.LINE, "line", value);
    }

}
