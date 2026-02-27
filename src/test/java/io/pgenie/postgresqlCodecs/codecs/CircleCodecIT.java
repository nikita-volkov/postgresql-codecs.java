package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

public class CircleCodecIT extends CodecITBase {

    @Test
    void circleRoundTrip() throws Exception {
        var circle = new org.postgresql.geometric.PGcircle(1.0, 2.0, 3.0);
        var result = roundTrip(Codec.CIRCLE, "circle", circle);
        assertNotNull(result);
        assertEquals(circle.center.x, result.center.x, 0.0001);
        assertEquals(circle.center.y, result.center.y, 0.0001);
        assertEquals(circle.radius, result.radius, 0.0001);
    }
}
