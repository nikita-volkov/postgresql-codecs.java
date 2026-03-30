package io.codemine.postgresql.codecs;

public class BpcharCodecIT extends CodecITBase<String> {
  public BpcharCodecIT() {
    super(Codec.BPCHAR, String.class);
  }
}
