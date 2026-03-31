package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/** Codec for PostgreSQL {@code bpchar} (blank-padded character) values. */
final class BpcharCodec implements Codec<String> {

  private final int size;

  BpcharCodec() {
    this(0);
  }

  BpcharCodec(int size) {
    if (size < 0) {
      throw new IllegalArgumentException("size must be >= 0, got: " + size);
    }
    this.size = size;
  }

  @Override
  public String name() {
    return "bpchar";
  }

  @Override
  public String typeSig() {
    return size > 0 ? "bpchar(" + size + ")" : "bpchar";
  }

  @Override
  public int scalarOid() {
    return 1042;
  }

  @Override
  public int arrayOid() {
    return 1014;
  }

  @Override
  public void encodeInText(StringBuilder sb, String value) {
    sb.append(value);
  }

  @Override
  public Codec.ParsingResult<String> decodeInText(CharSequence input, int offset) {
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
  public String random(Random r, int randomSize) {
    int len = size > 0 ? size : randomSize;
    final int range1Size = 0xD7FF;
    final int totalValid = range1Size + (0x10FFFF - 0xE000 + 1);
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      int n = r.nextInt(totalValid);
      int codePoint = (n < range1Size) ? n + 1 : n + (0xE000 - range1Size);
      sb.appendCodePoint(codePoint);
    }
    return sb.toString();
  }
}
