package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/** Codec for PostgreSQL {@code text} values. */
public final class TextCodec implements Codec<String> {

  @Override
  public String name() {
    return "text";
  }

  @Override
  public int scalarOid() {
    return 25;
  }

  @Override
  public int arrayOid() {
    return 1009;
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
  public String random(Random r) {
    String chars = "abcdefghijklmnopqABCDEF0123,()\"\\";
    int len = r.nextInt(10);
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(chars.charAt(r.nextInt(chars.length())));
    }
    return sb.toString();
  }
}
