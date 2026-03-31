package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/** Codec for PostgreSQL {@code citext} values (case-insensitive text extension type). */
final class CitextCodec implements Codec<String> {

  @Override
  public String name() {
    return "citext";
  }

  @Override
  public int scalarOid() {
    return 0;
  }

  @Override
  public int arrayOid() {
    return 0;
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
  public String random(Random r, int size) {
    // Valid code points: U+0001..U+D7FF and U+E000..U+10FFFF.
    // U+0000 is excluded because PostgreSQL text cannot contain null bytes.
    // U+D800..U+DFFF are excluded because they are surrogates, invalid in UTF-8.
    final int range1Size = 0xD7FF; // U+0001..U+D7FF → 55295 code points
    final int totalValid = range1Size + (0x10FFFF - 0xE000 + 1); // 1112063 code points
    StringBuilder sb = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      int n = r.nextInt(totalValid);
      int codePoint = (n < range1Size) ? n + 1 : n + (0xE000 - range1Size);
      sb.appendCodePoint(codePoint);
    }
    return sb.toString();
  }
}
