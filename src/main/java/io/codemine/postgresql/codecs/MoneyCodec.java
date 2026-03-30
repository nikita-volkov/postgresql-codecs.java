package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
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

  /**
   * Writes the money value as a plain decimal (e.g. {@code 1234.56} or {@code -1234.56}) without
   * any currency symbol or grouping separators, which PostgreSQL accepts on all locales.
   */
  @Override
  public void write(StringBuilder sb, Long value) {
    // BigDecimal.valueOf(unscaled, scale) is safe for all long values including Long.MIN_VALUE.
    sb.append(BigDecimal.valueOf(value, 2).toPlainString());
  }

  /**
   * Parses PostgreSQL money text output, which is locale-dependent (e.g. {@code $1,234.56}, {@code
   * €1.234,56}). This implementation:
   *
   * <ul>
   *   <li>Handles both {@code -value} and {@code (value)} negative notations.
   *   <li>Strips all currency symbols and other non-numeric characters.
   *   <li>Auto-detects the decimal separator: whichever of {@code .} or {@code ,} appears last and
   *       is followed by exactly 2 digits is treated as the decimal separator; the other is a
   *       grouping separator.
   * </ul>
   */
  @Override
  public Codec.ParsingResult<Long> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      boolean negative = false;

      // Handle parentheses (accounting negative format, e.g. "($1.23)").
      if (s.startsWith("(") && s.endsWith(")")) {
        negative = true;
        s = s.substring(1, s.length() - 1).trim();
      }
      // Handle leading '-'.
      if (s.startsWith("-")) {
        negative = true;
        s = s.substring(1).trim();
      }

      // Strip everything except digits, '.' and ','.
      s = s.replaceAll("[^0-9.,]", "");

      long dollars;
      long cents;

      int lastDot = s.lastIndexOf('.');
      int lastComma = s.lastIndexOf(',');
      int lastSep = Math.max(lastDot, lastComma);

      if (lastSep >= 0 && (s.length() - lastSep - 1) == 2) {
        // The last separator is followed by exactly 2 digits → it is the decimal separator.
        char grpSep = (s.charAt(lastSep) == '.') ? ',' : '.';
        String intPart = s.substring(0, lastSep).replace(String.valueOf(grpSep), "");
        String fracPart = s.substring(lastSep + 1);
        dollars = intPart.isEmpty() ? 0L : Long.parseLong(intPart);
        cents = Long.parseLong(fracPart);
      } else {
        // No recognisable decimal separator — treat the whole string as integer dollars.
        String digits = s.replaceAll("[.,]", "");
        dollars = digits.isEmpty() ? 0L : Long.parseLong(digits);
        cents = 0L;
      }

      long value = dollars * 100L + cents;
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
