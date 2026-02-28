package io.pgenie.postgresqlcodecs.arbitrary;

import io.pgenie.postgresqlcodecs.types.Inet;
import io.pgenie.postgresqlcodecs.types.Macaddr;
import net.jqwik.api.Arbitrary;

public final class Arbitraries {

  private Arbitraries() {}

  public static Arbitrary<Inet> inet() {
    return net.jqwik.api.Arbitraries.randomValue(Inet::generate);
  }

  public static Arbitrary<Macaddr> macaddr() {
    return net.jqwik.api.Arbitraries.randomValue(Macaddr::generate);
  }
}
