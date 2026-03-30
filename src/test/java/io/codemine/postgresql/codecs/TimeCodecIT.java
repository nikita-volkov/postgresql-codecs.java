package io.codemine.postgresql.codecs;

public class TimeCodecIT extends CodecITBase<Long> {
  public TimeCodecIT() {
    super(Codec.TIME, Long.class);
  }
}
