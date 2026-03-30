package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.util.Random;

/** Codec for PostgreSQL {@code time} values, represented as {@link LocalTime}. */
final class TimeCodec implements Codec<LocalTime> {

  @Override
  public String name() {
    return "time";
  }

  @Override
  public int scalarOid() {
    return 1083;
  }

  @Override
  public int arrayOid() {
    return 1183;
  }

  @Override
  public void write(StringBuilder sb, LocalTime value) {
    long micros = value.toNanoOfDay() / 1_000L;
    writeTime(sb, micros);
  }

  @Override
  public Codec.ParsingResult<LocalTime> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      long micros = parseTime(s, 0);
      return new Codec.ParsingResult<>(LocalTime.ofNanoOfDay(micros * 1_000L), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid time: " + s);
    }
  }

  @Override
  public void encodeInBinary(LocalTime value, ByteArrayOutputStream out) {
    long micros = value.toNanoOfDay() / 1_000L;
    out.write((int) (micros >>> 56) & 0xFF);
    out.write((int) (micros >>> 48) & 0xFF);
    out.write((int) (micros >>> 40) & 0xFF);
    out.write((int) (micros >>> 32) & 0xFF);
    out.write((int) (micros >>> 24) & 0xFF);
    out.write((int) (micros >>> 16) & 0xFF);
    out.write((int) (micros >>> 8) & 0xFF);
    out.write((int) (micros & 0xFF));
  }

  @Override
  public LocalTime decodeInBinary(ByteBuffer buf, int length) {
    long micros = buf.getLong();
    return LocalTime.ofNanoOfDay(micros * 1_000L);
  }

  @Override
  public LocalTime random(Random r, int size) {
    long micros = r.nextLong(0, 86_400_000_000L);
    return LocalTime.ofNanoOfDay(micros * 1_000L);
  }

  /** Writes a time-of-day in microseconds to the StringBuilder as hh:mm:ss[.ffffff]. */
  private static void writeTime(StringBuilder sb, long micros) {
    long total = micros;
    long hours = total / 3_600_000_000L;
    total %= 3_600_000_000L;
    long minutes = total / 60_000_000L;
    total %= 60_000_000L;
    long seconds = total / 1_000_000L;
    long frac = total % 1_000_000L;

    pad2(sb, hours);
    sb.append(':');
    pad2(sb, minutes);
    sb.append(':');
    pad2(sb, seconds);
    appendFraction(sb, frac);
  }

  /** Appends fractional seconds (1-6 digits, trailing zeros stripped) if non-zero. */
  private static void appendFraction(StringBuilder sb, long micros) {
    if (micros > 0) {
      sb.append('.');
      int val = (int) micros;
      sb.append((char) ('0' + val / 100000));
      sb.append((char) ('0' + val / 10000 % 10));
      sb.append((char) ('0' + val / 1000 % 10));
      sb.append((char) ('0' + val / 100 % 10));
      sb.append((char) ('0' + val / 10 % 10));
      sb.append((char) ('0' + val % 10));
      int len = sb.length();
      while (sb.charAt(len - 1) == '0') len--;
      sb.setLength(len);
    }
  }

  /** Appends a zero-padded 2-digit integer. */
  private static void pad2(StringBuilder sb, long v) {
    if (v < 10) sb.append('0');
    sb.append(v);
  }

  /**
   * Parses a time string (hh:mm:ss[.ffffff]) starting at the given position and returns the time in
   * microseconds.
   */
  private static long parseTime(CharSequence input, int pos) {
    String s = input.subSequence(pos, input.length()).toString();
    String[] parts = s.split(":");
    if (parts.length < 3) {
      throw new IllegalArgumentException("Invalid time: " + s);
    }
    long hours = Long.parseLong(parts[0]);
    long minutes = Long.parseLong(parts[1]);
    // seconds may have fractional part
    String secPart = parts[2];
    long seconds;
    long micros = 0;
    int dot = secPart.indexOf('.');
    if (dot >= 0) {
      seconds = Long.parseLong(secPart.substring(0, dot));
      String frac = secPart.substring(dot + 1);
      // Pad to 6 digits
      while (frac.length() < 6) {
        frac = frac + "0";
      }
      if (frac.length() > 6) {
        frac = frac.substring(0, 6);
      }
      micros = Long.parseLong(frac);
    } else {
      seconds = Long.parseLong(secPart);
    }
    return hours * 3_600_000_000L + minutes * 60_000_000L + seconds * 1_000_000L + micros;
  }
}
