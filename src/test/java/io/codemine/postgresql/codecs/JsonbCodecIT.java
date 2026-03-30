package io.codemine.postgresql.codecs;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonbCodecIT extends CodecITBase<JsonNode> {
  public JsonbCodecIT() {
    super(Codec.JSONB, JsonNode.class);
  }
}
