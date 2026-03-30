package io.codemine.postgresql.codecs;

public class MacaddrCodecIT extends CodecITBase<Macaddr> {
  public MacaddrCodecIT() {
    super(Codec.MACADDR, Macaddr.class);
  }
}
