package io.codemine.postgresql.codecs;

import io.codemine.postgresql.types.Macaddr;

public class MacaddrCodecIT extends CodecITSuite<Macaddr> {
  public MacaddrCodecIT() {
    super(Macaddr.CODEC, Macaddr.class);
  }
}
