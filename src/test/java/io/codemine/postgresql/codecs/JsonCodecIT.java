package io.codemine.postgresql.codecs;

public class JsonCodecIT extends CodecITBase<String> {
  public JsonCodecIT() {
    super(Codec.JSON, String.class);
  }
}
