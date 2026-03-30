package io.codemine.postgresql.codecs;

public class DateCodecIT extends CodecITBase<Integer> {
  public DateCodecIT() {
    super(Codec.DATE, Integer.class);
  }
}
