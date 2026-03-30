package io.codemine.postgresql.codecs;

import java.math.BigDecimal;

public class NumericCodecTest extends CodecTestBase<BigDecimal> {
  public NumericCodecTest() {
    super(Codec.NUMERIC);
  }
}
