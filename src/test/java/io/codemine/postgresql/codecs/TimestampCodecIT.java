package io.codemine.postgresql.codecs;

public class TimestampCodecIT extends CodecITBase<Long> {
  public TimestampCodecIT() {
    super(Codec.TIMESTAMP, Long.class);
  }
}
