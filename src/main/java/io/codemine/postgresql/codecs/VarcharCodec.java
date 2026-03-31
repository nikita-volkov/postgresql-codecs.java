package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/** Codec for PostgreSQL {@code varchar} values. */
final class VarcharCodec implements Codec<String> {

  private final int maxSize;

  VarcharCodec() {
    this(0);
  }

  VarcharCodec(int maxSize) {
    if (maxSize < 0) throw new IllegalArgumentException("maxSize must be >= 0, got: " + maxSize);
    this.maxSize = maxSize;
  }

  @Override
  public String name() {
    return "varchar";
  }

  @Override
  public String typeSig() {
    return maxSize > 0 ? "varchar(" + maxSize + ")" : "varchar";
  }

  @Override
  public int scalarOid() {
    return 1043;
  }

  @Override
  public int arrayOid() {
    return 1015;
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
    int effectiveMax;
    if (maxSize > 0) {
      effectiveMax = Math.min(maxSize, randomSize);
    } else {
      effectiveMax = randomSize;
    }
    if (effectiveMax < 0) {
      effectiveMax = 0;
    }
    int len = r.nextInt(-1, effectiveMax) + 1;
    // Reuse TextCodec's random string generation logic.
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
