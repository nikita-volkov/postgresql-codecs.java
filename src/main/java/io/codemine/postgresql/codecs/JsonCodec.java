package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/** Codec for PostgreSQL {@code json} values. */
final class JsonCodec implements Codec<String> {

  @Override
  public String name() {
    return "json";
  }

  @Override
  public int scalarOid() {
    return 114;
  }

  @Override
  public int arrayOid() {
    return 199;
  }

  @Override
  public void write(StringBuilder sb, String value) {
    sb.append(value);
  }

  @Override
  public Codec.ParsingResult<String> parse(CharSequence input, int offset) {
    return new Codec.ParsingResult<>(
        input.subSequence(offset, input.length()).toString(), input.length());
  }

  @Override
  public void encodeInBinary(String value, ByteArrayOutputStream out) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.write(bytes, 0, bytes.length);
  }

  @Override
  public String decodeInBinary(ByteBuffer buf, int length) {
    byte[] bytes = new byte[length];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public String random(Random r, int size) {
    return randomJson(r, size);
  }

  private static String randomJson(Random r, int size) {
    int choice = r.nextInt(4);
    return switch (choice) {
      case 0 -> String.valueOf(r.nextInt(size + 1));
      case 1 -> "\"" + randomAlphanumeric(r, Math.max(1, size)) + "\"";
      case 2 -> {
        int n = Math.min(r.nextInt(Math.max(1, size)) + 1, 3);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < n; i++) {
          if (i > 0) sb.append(",");
          sb.append(r.nextInt(1000));
        }
        sb.append("]");
        yield sb.toString();
      }
      default -> {
        int n = Math.min(r.nextInt(Math.max(1, size)) + 1, 3);
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < n; i++) {
          if (i > 0) sb.append(",");
          sb.append("\"k").append(i).append("\":").append(r.nextInt(1000));
        }
        sb.append("}");
        yield sb.toString();
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
