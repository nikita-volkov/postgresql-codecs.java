package io.codemine.postgresql.codecs;

public class CidrCodecIT extends CodecITBase<Cidr> {
  public CidrCodecIT() {
    super(Codec.CIDR, Cidr.class);
  }
}
