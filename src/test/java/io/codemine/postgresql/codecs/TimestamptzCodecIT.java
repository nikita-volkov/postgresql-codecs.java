package io.codemine.postgresql.codecs;

public class TimestamptzCodecIT extends CodecITBase<Long> {
  public TimestamptzCodecIT() {
    super(Codec.TIMESTAMPTZ, Long.class);
  }
}
