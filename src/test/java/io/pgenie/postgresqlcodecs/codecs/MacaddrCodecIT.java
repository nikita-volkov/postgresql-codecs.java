package io.pgenie.postgresqlcodecs.codecs;

import io.pgenie.postgresqlcodecs.types.Macaddr;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Provide;

public class MacaddrCodecIT extends CodecSuite<Macaddr> {

  public MacaddrCodecIT() {
    super(Macaddr.CODEC, Macaddr.class);
  }

  @Provide
  Arbitrary<Macaddr> values() {
    return net.jqwik.api.Arbitraries.randomValue(Macaddr::generate);
  }
}
