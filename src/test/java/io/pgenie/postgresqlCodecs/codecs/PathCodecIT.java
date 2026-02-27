package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class PathCodecIT extends CodecITBase {

    @Test
    void pathBinary() throws Exception {
        var openPath = new org.postgresql.geometric.PGpath(
                new org.postgresql.geometric.PGpoint[]{
                        new org.postgresql.geometric.PGpoint(0, 0),
                        new org.postgresql.geometric.PGpoint(1, 0),
                        new org.postgresql.geometric.PGpoint(1, 1)
                }, true);
        byte[] pgBytes = pgBinaryBytes(Codec.PATH, "path", openPath);
        assertEquals(hex(pgBytes), hex(Codec.PATH.encode(openPath)));
        var decoded = Codec.PATH.decodeBinary(wrap(pgBytes), pgBytes.length);
        assertEquals(3, decoded.points.length);
        assertTrue(decoded.open);
    }
}
