package io.codemine.postgresql.codecs;

public class CharCodecIT extends CodecITBase<Byte> {
  public CharCodecIT() {
    super(Codec.CHAR, Byte.class);
  }
}
