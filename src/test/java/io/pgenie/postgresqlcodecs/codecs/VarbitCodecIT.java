package io.pgenie.postgresqlcodecs.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.pgenie.postgresqlcodecs.types.Varbit;

import io.pgenie.postgresqlcodecs.types.Varbit;

public class VarbitCodecIT extends CodecITBase {

    @Test
    void varbitRoundTrip() throws Exception {
        var varbit = Varbit.fromBitString("1011010");
        assertEquals(varbit, roundTrip(Codec.VARBIT, varbit));
    }

    @Test
    void varbitNull() throws Exception {
        assertNull(roundTrip(Codec.VARBIT, null));
    }

    @Test
    void varbitOid() throws Exception {
        assertOid(Codec.VARBIT);
    }

    @Test
    void varbitBinary() throws Exception {
        var value = Varbit.fromBitString("10110");
        byte[] pgBytes = pgBinaryBytes(Codec.VARBIT, "varbit", value);
        assertEquals(hex(pgBytes), hex(Codec.VARBIT.encode(value)));
        assertEquals(value, Codec.VARBIT.decodeBinary(wrap(pgBytes), pgBytes.length));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#varbits")
    void varbitPropertyRoundTrip(Varbit value) throws Exception {
        assertEquals(value, roundTrip(Codec.VARBIT, value));
    }

    @ParameterizedTest
    @MethodSource("io.pgenie.postgresqlcodecs.codecs.Generators#varbits")
    void varbitPropertyBinaryRoundTrip(Varbit value) throws Exception {
        assertBinaryRoundTrip(Codec.VARBIT, "varbit", value);
    }

}
