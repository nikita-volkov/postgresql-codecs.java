package io.pgenie.postgresqlcodecs.codecs;

import io.pgenie.postgresqlcodecs.types.Inet;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Provide;

public class InetCodecIT extends CodecSuite<Inet> {

  public InetCodecIT() {
    super(Inet.CODEC, Inet.class);
  }

  @Provide
  Arbitrary<Inet> values() {
    return net.jqwik.api.Arbitraries.randomValue(Inet::generate);
  }
}
