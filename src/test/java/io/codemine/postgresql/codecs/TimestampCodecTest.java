package io.codemine.postgresql.codecs;

import java.time.LocalDateTime;

public class TimestampCodecTest extends CodecTestBase<LocalDateTime> {
  public TimestampCodecTest() {
    super(Codec.TIMESTAMP);
  }
}
