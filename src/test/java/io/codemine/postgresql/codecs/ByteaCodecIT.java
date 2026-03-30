package io.codemine.postgresql.codecs;

public class ByteaCodecIT extends CodecITBase<Bytea> {
  public ByteaCodecIT() {
    super(Codec.BYTEA, Bytea.class);
  }
}
