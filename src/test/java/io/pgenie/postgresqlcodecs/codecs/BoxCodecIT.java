package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGbox;

public class BoxCodecIT extends CodecITBase {

    @Test
    void boxRoundTrip() throws Exception {
        var box = new PGbox(3.0, 4.0, 1.0, 2.0);
        var result = roundTrip(Codec.BOX, box);
        assertNotNull(result);
    }


    @Test
    void boxOid() throws Exception {
        assertOid(Codec.BOX);
    }

    @Test
    void boxBinary() throws Exception {
        assertBinaryRoundTrip(Codec.BOX, "box", new PGbox(2, 2, 0, 0));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#boxes")
    void boxPropertyBinaryRoundTrip(PGbox value) throws Exception {
        assertBinaryRoundTrip(Codec.BOX, "box", value);
    }

}
