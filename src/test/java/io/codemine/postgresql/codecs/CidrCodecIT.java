package io.codemine.postgresql.codecs;

public class CidrCodecIT extends CodecITBase<Inet> {
  public CidrCodecIT() {
    super(Codec.CIDR, Inet.class);
  }
}
