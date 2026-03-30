package io.codemine.postgresql.codecs;

import java.time.LocalTime;

public class TimeCodecTest extends CodecTestBase<LocalTime> {
  public TimeCodecTest() {
    super(Codec.TIME);
  }
}
