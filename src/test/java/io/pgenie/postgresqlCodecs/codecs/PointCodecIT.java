package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class PointCodecIT extends CodecITBase {

    @Test
    void pointRoundTrip() throws Exception {
        var pt = new org.postgresql.geometric.PGpoint(1.5, 2.5);
        var result = roundTrip(Codec.POINT, "point", pt);
        assertEquals(pt.x, result.x, 0.0001);
        assertEquals(pt.y, result.y, 0.0001);
    }

    @Test
    void pointNull() throws Exception {
        assertNull(roundTrip(Codec.POINT, "point", null));
    }
}
