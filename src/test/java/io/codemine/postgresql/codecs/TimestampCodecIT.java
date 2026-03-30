package io.codemine.postgresql.codecs;

import java.time.LocalDateTime;

public class TimestampCodecIT extends CodecITBase<LocalDateTime> {
  public TimestampCodecIT() {
    super(Codec.TIMESTAMP, LocalDateTime.class);
  }
}
