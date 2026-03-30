package io.codemine.postgresql.codecs;

public class JsonbCodecIT extends CodecITBase<String> {
  public JsonbCodecIT() {
    super(Codec.JSONB, String.class);
  }
}
