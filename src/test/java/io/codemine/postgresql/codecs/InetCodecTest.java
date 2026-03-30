package io.codemine.postgresql.codecs;

public class InetCodecTest extends CodecTestBase<Inet> {
  public InetCodecTest() {
    super(Codec.INET, Inet.class);
  }
}
