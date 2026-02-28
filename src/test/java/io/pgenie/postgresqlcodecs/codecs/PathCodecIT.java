package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;

public class PathCodecIT extends CodecITBase {

    @Test
    void pathBinary() throws Exception {
        var openPath = new PGpath(
                new PGpoint[]{
                        new PGpoint(0, 0),
                        new PGpoint(1, 0),
                        new PGpoint(1, 1)
                }, true);
        byte[] pgBytes = pgBinaryBytes(Codec.PATH, "path", openPath);
        assertEquals(hex(pgBytes), hex(Codec.PATH.encode(openPath)));
        var decoded = Codec.PATH.decodeBinary(wrap(pgBytes), pgBytes.length);
        assertEquals(3, decoded.points.length);
        assertTrue(decoded.open);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#paths")
    void pathPropertyBinaryRoundTrip(PGpath value) throws Exception {
        assertBinaryRoundTrip(Codec.PATH, "path", value);
    }

}
