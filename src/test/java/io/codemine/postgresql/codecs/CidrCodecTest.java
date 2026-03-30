package io.codemine.postgresql.codecs;

public class CidrCodecTest extends CodecTestBase<Inet> {
  public CidrCodecTest() {
    super(Codec.CIDR);
  }
}
