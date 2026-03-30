package io.codemine.postgresql.codecs;

import java.time.Instant;

public class TimestamptzCodecIT extends CodecITBase<Instant> {
  public TimestamptzCodecIT() {
    super(Codec.TIMESTAMPTZ, Instant.class);
  }
}
