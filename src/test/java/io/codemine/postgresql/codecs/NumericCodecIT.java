package io.codemine.postgresql.codecs;

import java.math.BigDecimal;

public class NumericCodecIT extends CodecITBase<BigDecimal> {
  public NumericCodecIT() {
    super(Codec.NUMERIC, BigDecimal.class);
  }
}
