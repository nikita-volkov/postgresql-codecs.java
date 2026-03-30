package io.codemine.postgresql.codecs;

public class BoolCodecIT extends CodecITBase<Boolean> {
  public BoolCodecIT() {
    super(Codec.BOOL, Boolean.class);
  }
}
