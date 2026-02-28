package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;

public class PolygonCodecIT extends CodecITBase {

    @Test
    void polygonBinary() throws Exception {
        var poly = new PGpolygon(
                new PGpoint[]{
                        new PGpoint(0, 0),
                        new PGpoint(1, 0),
                        new PGpoint(0.5, 1)
                });
        byte[] pgBytes = pgBinaryBytes(Codec.POLYGON, "polygon", poly);
        assertEquals(hex(pgBytes), hex(Codec.POLYGON.encode(poly)));
        var decoded = Codec.POLYGON.decodeBinary(wrap(pgBytes), pgBytes.length);
        assertEquals(3, decoded.points.length);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#polygons")
    void polygonPropertyBinaryRoundTrip(PGpolygon value) throws Exception {
        assertBinaryRoundTrip(Codec.POLYGON, "polygon", value);
    }

}
