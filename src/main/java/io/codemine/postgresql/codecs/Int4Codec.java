package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code int4} values. */
public final class Int4Codec implements Codec<Integer> {

  @Override
  public String name() {
    return "int4";
  }

  @Override
  public int scalarOid() {
    return 23;
  }

  @Override
  public int arrayOid() {
    return 1007;
  }

  @Override
  public void write(StringBuilder sb, Integer value) {
    sb.append(value);
  }

  @Override
  public Codec.ParsingResult<Integer> parse(CharSequence input, int offset)
      throws Codec.ParseException {
    try {
      int value = Integer.parseInt(input.subSequence(offset, input.length()).toString().trim());
      return new Codec.ParsingResult<>(value, input.length());
    } catch (NumberFormatException e) {
      throw new Codec.ParseException(input, offset, "Invalid int4: " + e.getMessage());
    }
  }

  @Override
  public void encodeInBinary(Integer value, ByteArrayOutputStream out) {
    out.write((value >>> 24) & 0xFF);
    out.write((value >>> 16) & 0xFF);
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  @Override
  public Integer decodeInBinary(ByteBuffer buf, int length) {
    return buf.getInt();
  }

  @Override
  public Integer random(Random r) {
    return r.nextInt(10_000) - 5_000;
  }
}
