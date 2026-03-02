package io.codemine.postgresql.codecs;

import io.codemine.postgresql.types.Inet;

public class InetCodecIT extends CodecITSuite<Inet> {
  public InetCodecIT() {
    super(Inet.CODEC, Inet.class);
  }
}
