package io.codemine.postgresql.codecs;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonCodecIT extends CodecITBase<JsonNode> {
  public JsonCodecIT() {
    super(Codec.JSON, JsonNode.class);
  }
}
