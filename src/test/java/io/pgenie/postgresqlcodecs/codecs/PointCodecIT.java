package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGpoint;

public class PointCodecIT extends CodecITBase {

    @Test
    void pointRoundTrip() throws Exception {
        var pt = new PGpoint(1.5, 2.5);
        var result = roundTrip(Codec.POINT, pt);
        assertEquals(pt.x, result.x, 0.0001);
        assertEquals(pt.y, result.y, 0.0001);
    }

    @Test
    void pointNull() throws Exception {
        assertNull(roundTrip(Codec.POINT, null));
    }


    @Test
    void pointOid() throws Exception {
        assertOid(Codec.POINT);
    }

    @Test
    void pointBinary() throws Exception {
        assertBinaryRoundTrip(Codec.POINT, "point", new PGpoint(1.5, -2.5));
        assertBinaryRoundTrip(Codec.POINT, "point", new PGpoint(0, 0));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#points")
    void pointPropertyBinaryRoundTrip(PGpoint value) throws Exception {
        assertBinaryRoundTrip(Codec.POINT, "point", value);
    }

}
