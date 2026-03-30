package io.codemine.postgresql.codecs;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonbCodecTest extends CodecTestBase<JsonNode> {
  public JsonbCodecTest() {
    super(Codec.JSONB);
  }
}
