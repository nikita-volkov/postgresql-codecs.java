package io.codemine.postgresql.codecs;

public class InetCodecIT extends CodecITBase<Inet> {
  public InetCodecIT() {
    super(Codec.INET, Inet.class);
  }
}
