package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code money} values. */
final class MoneyCodec implements Codec<Long> {

  @Override
  public String name() {
    return "money";
  }

  @Override
  public int scalarOid() {
    return 790;
  }

  @Override
  public int arrayOid() {
    return 791;
  }

  @Override
  public void write(StringBuilder sb, Long value) {
    boolean negative = value < 0;
    long abs = Math.abs(value);
    long dollars = abs / 100;
    long cents = abs % 100;
    if (negative) {
      sb.append("-");
    }
    sb.append("$").append(dollars).append(".").append(String.format("%02d", cents));
  }

  @Override
  public Codec.ParsingResult<Long> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      // Handle negative in parentheses: ($1.23) or -$1.23
      boolean negative = false;
      if (s.startsWith("(") && s.endsWith(")")) {
        negative = true;
        s = s.substring(1, s.length() - 1);
      }
      if (s.startsWith("-")) {
        negative = true;
        s = s.substring(1);
      }
      // Strip currency symbol and commas.
      s = s.replace("$", "").replace(",", "");
      // Parse as decimal and convert to cents.
      int dotIndex = s.indexOf('.');
      long value;
      if (dotIndex < 0) {
        value = Long.parseLong(s) * 100;
      } else {
        String intPart = s.substring(0, dotIndex);
        String fracPart = s.substring(dotIndex + 1);
        // Pad or truncate to 2 decimal places.
        if (fracPart.length() == 1) {
          fracPart = fracPart + "0";
        } else if (fracPart.length() > 2) {
          fracPart = fracPart.substring(0, 2);
        }
        value = Long.parseLong(intPart) * 100 + Long.parseLong(fracPart);
      }
      if (negative) {
        value = -value;
      }
      return new Codec.ParsingResult<>(value, input.length());
    } catch (NumberFormatException e) {
      throw new Codec.DecodingException(input, offset, "Invalid money: " + e.getMessage());
    }
  }

  @Override
  public void encodeInBinary(Long value, ByteArrayOutputStream out) {
    out.write((int) (value >>> 56) & 0xFF);
    out.write((int) (value >>> 48) & 0xFF);
    out.write((int) (value >>> 40) & 0xFF);
    out.write((int) (value >>> 32) & 0xFF);
    out.write((int) (value >>> 24) & 0xFF);
    out.write((int) (value >>> 16) & 0xFF);
    out.write((int) (value >>> 8) & 0xFF);
    out.write((int) (value & 0xFF));
  }

  @Override
  public Long decodeInBinary(ByteBuffer buf, int length) {
    return buf.getLong();
  }

  @Override
  public Long random(Random r, int size) {
    if (size == 0) {
      return 0L;
    }
    return r.nextLong(-((long) size * 100), (long) size * 100 + 1);
  }
}
