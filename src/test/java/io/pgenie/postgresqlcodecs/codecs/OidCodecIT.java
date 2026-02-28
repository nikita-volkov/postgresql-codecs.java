package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class OidCodecIT extends CodecITBase {

    @Test
    void oidRoundTrip() throws Exception {
        assertEquals(12345L, roundTrip(Codec.OID, 12345L));
    }

    @Test
    void oidNull() throws Exception {
        assertNull(roundTrip(Codec.OID, null));
    }


    @Test
    void oidOid() throws Exception {
        assertOid(Codec.OID);
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.arbitrary.Arbitrary#oids")
    void oidPropertyRoundTrip(Long value) throws Exception {
        assertEquals(value, roundTrip(Codec.OID, value));
    }

}
