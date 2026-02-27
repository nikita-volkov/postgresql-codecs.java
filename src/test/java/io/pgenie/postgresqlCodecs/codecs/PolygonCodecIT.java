package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class PolygonCodecIT extends CodecITBase {

    @Test
    void polygonBinary() throws Exception {
        var poly = new org.postgresql.geometric.PGpolygon(
                new org.postgresql.geometric.PGpoint[]{
                        new org.postgresql.geometric.PGpoint(0, 0),
                        new org.postgresql.geometric.PGpoint(1, 0),
                        new org.postgresql.geometric.PGpoint(0.5, 1)
                });
        byte[] pgBytes = pgBinaryBytes(Codec.POLYGON, "polygon", poly);
        assertEquals(hex(pgBytes), hex(Codec.POLYGON.encode(poly)));
        var decoded = Codec.POLYGON.decodeBinary(wrap(pgBytes), pgBytes.length);
        assertEquals(3, decoded.points.length);
    }
}
