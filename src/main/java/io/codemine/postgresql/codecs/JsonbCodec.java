package io.codemine.postgresql.codecs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code jsonb} values, represented as Jackson {@link JsonNode}. */
final class JsonbCodec implements Codec<JsonNode> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String name() {
    return "jsonb";
  }

  @Override
  public int scalarOid() {
    return 3802;
  }

  @Override
  public int arrayOid() {
    return 3807;
  }

  @Override
  public void write(StringBuilder sb, JsonNode value) {
    sb.append(value.toString());
  }

  @Override
  public Codec.ParsingResult<JsonNode> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString();
    try {
      return new Codec.ParsingResult<>(MAPPER.readTree(s), input.length());
    } catch (JsonProcessingException e) {
      throw new Codec.DecodingException(input, offset, "Invalid JSON: " + e.getMessage());
    }
  }

  @Override
  public void encodeInBinary(JsonNode value, ByteArrayOutputStream out) {
    try {
      out.write(1); // JSONB version byte
      byte[] bytes = MAPPER.writeValueAsBytes(value);
      out.write(bytes, 0, bytes.length);
    } catch (IOException e) {
      throw new RuntimeException("Failed to encode JSONB", e);
    }
  }

  @Override
  public JsonNode decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    byte version = buf.get();
    if (version != 1) {
      throw new Codec.DecodingException("Unsupported jsonb version: " + version);
    }
    byte[] bytes = new byte[length - 1];
    buf.get(bytes);
    try {
      return MAPPER.readTree(bytes);
    } catch (IOException e) {
      throw new Codec.DecodingException("Invalid JSONB binary: " + e.getMessage());
    }
  }

  @Override
  public JsonNode random(Random r, int size) {
    return randomJsonNode(r, size, 0);
  }

  private static JsonNode randomJsonNode(Random r, int size, int depth) {
    // At depth >= 2 or size 0, produce a scalar to bound recursion
    if (depth >= 2 || size == 0) {
      int choice = r.nextInt(2);
      if (choice == 0) {
        return MAPPER.getNodeFactory().numberNode(r.nextInt(size + 1));
      } else {
        return MAPPER.getNodeFactory().textNode(randomAlphanumeric(r, Math.max(1, size)));
      }
    }
    int choice = r.nextInt(4);
    return switch (choice) {
      case 0 -> MAPPER.getNodeFactory().numberNode(r.nextInt(size + 1));
      case 1 -> MAPPER.getNodeFactory().textNode(randomAlphanumeric(r, Math.max(1, size)));
      case 2 -> {
        int n = Math.min(r.nextInt(Math.max(1, size)) + 1, 3);
        ArrayNode arr = MAPPER.createArrayNode();
        for (int i = 0; i < n; i++) {
          arr.add(randomJsonNode(r, size - 1, depth + 1));
        }
        yield arr;
      }
      default -> {
        int n = Math.min(r.nextInt(Math.max(1, size)) + 1, 3);
        ObjectNode obj = MAPPER.createObjectNode();
        for (int i = 0; i < n; i++) {
          obj.set("k" + i, randomJsonNode(r, size - 1, depth + 1));
        }
        yield obj;
      }
    };
  }

  private static String randomAlphanumeric(Random r, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      int c = r.nextInt(36);
      sb.append((char) (c < 10 ? '0' + c : 'a' + c - 10));
    }
    return sb.toString();
  }
}
