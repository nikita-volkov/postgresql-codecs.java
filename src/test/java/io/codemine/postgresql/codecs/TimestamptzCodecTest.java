package io.codemine.postgresql.codecs;

import java.time.Instant;

public class TimestamptzCodecTest extends CodecTestBase<Instant> {
  public TimestamptzCodecTest() {
    super(Codec.TIMESTAMPTZ);
  }
}
