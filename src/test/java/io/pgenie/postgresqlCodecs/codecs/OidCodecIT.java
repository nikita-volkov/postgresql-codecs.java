package io.pgenie.postgresqlCodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

public class OidCodecIT extends CodecITBase {

    @Test
    void oidRoundTrip() throws Exception {
        assertEquals(12345L, roundTrip(Codec.OID, "oid", 12345L));
    }

    @Test
    void oidNull() throws Exception {
        assertNull(roundTrip(Codec.OID, "oid", null));
    }


    @Test
    void oidOid() throws Exception {
        assertOid(Codec.OID, "oid");
    }

}
