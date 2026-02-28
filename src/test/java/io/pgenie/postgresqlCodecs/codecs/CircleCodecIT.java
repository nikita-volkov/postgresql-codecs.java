package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGpoint;

public class CircleCodecIT extends CodecITBase {

    @Test
    void circleRoundTrip() throws Exception {
        var circle = new PGcircle(1.0, 2.0, 3.0);
        var result = roundTrip(Codec.CIRCLE, circle);
        assertNotNull(result);
        assertEquals(circle.center.x, result.center.x, 0.0001);
        assertEquals(circle.center.y, result.center.y, 0.0001);
        assertEquals(circle.radius, result.radius, 0.0001);
    }


    @Test
    void circleOid() throws Exception {
        assertOid(Codec.CIRCLE);
    }

    @Test
    void circleBinary() throws Exception {
        assertBinaryRoundTrip(Codec.CIRCLE, "circle",
                new PGcircle(new PGpoint(1.0, 2.0), 5.0));
    }

    /**
     * Property: arbitrary circles (with non-negative radius) round-trip through
     * the binary codec exactly.
     *
     * <p>Mirrors the Haskell {@code Circle} Arbitrary instance which enforces
     * {@code radius >= 0}.
     */
    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlCodecs.codecs.Generators#circles")
    void circlePropertyBinaryRoundTrip(PGcircle value) throws Exception {
        assertBinaryRoundTrip(Codec.CIRCLE, "circle", value);
    }

}
