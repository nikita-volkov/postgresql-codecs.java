package io.pgenie.postgresqlcodecs.codecs;

import io.pgenie.postgresqlcodecs.arbitrary.Arbitrary;
import io.pgenie.postgresqlcodecs.types.Macaddr;

public class MacaddrCodecIT extends CodecSuite<Macaddr> {

    public MacaddrCodecIT() {
        super(Macaddr.CODEC, Arbitrary.MACADDR, Macaddr.class);
    }
    
}
