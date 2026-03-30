package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code timetz} values (time with time zone). */
final class TimetzCodec implements Codec<Timetz> {

  private static final long MAX_TIME = 86_400_000_000L;

  @Override
  public String name() {
    return "timetz";
  }

  @Override
  public int scalarOid() {
    return 1266;
  }

  @Override
  public int arrayOid() {
    return 1270;
  }

  @Override
  public void write(StringBuilder sb, Timetz value) {
    // Write hh:mm:ss[.ffffff]
    long total = value.time();
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

    // Write timezone: internal zone has inverted sign (negative = UTC+)
    int displayOffset = -value.zone();
    char sign = displayOffset >= 0 ? '+' : '-';
    int abs = Math.abs(displayOffset);
    int tzHours = abs / 3600;
    int tzMinutes = (abs % 3600) / 60;
    int tzSeconds = abs % 60;
    sb.append(sign);
    pad2(sb, tzHours);
    if (tzMinutes != 0 || tzSeconds != 0) {
      sb.append(':');
      pad2(sb, tzMinutes);
      if (tzSeconds != 0) {
        sb.append(':');
        pad2(sb, tzSeconds);
      }
    }
  }

  @Override
  public Codec.ParsingResult<Timetz> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      // Find the timezone sign (+ or -) after the time part.
      // The time part is hh:mm:ss[.ffffff], then ±hh[:mm[:ss]]
      int tzStart = findTimezoneStart(s);
      String timePart = s.substring(0, tzStart);
      String tzPart = s.substring(tzStart);

      long time = parseTime(timePart);
      int zone = parseTimezone(tzPart);

      return new Codec.ParsingResult<>(new Timetz(time, zone), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid timetz: " + s);
    }
  }

  @Override
  public void encodeInBinary(Timetz value, ByteArrayOutputStream out) {
    long time = value.time();
    out.write((int) (time >>> 56) & 0xFF);
    out.write((int) (time >>> 48) & 0xFF);
    out.write((int) (time >>> 40) & 0xFF);
    out.write((int) (time >>> 32) & 0xFF);
    out.write((int) (time >>> 24) & 0xFF);
    out.write((int) (time >>> 16) & 0xFF);
    out.write((int) (time >>> 8) & 0xFF);
    out.write((int) (time & 0xFF));
    int zone = value.zone();
    out.write((zone >>> 24) & 0xFF);
    out.write((zone >>> 16) & 0xFF);
    out.write((zone >>> 8) & 0xFF);
    out.write(zone & 0xFF);
  }

  @Override
  public Timetz decodeInBinary(ByteBuffer buf, int length) {
    long time = buf.getLong();
    int zone = buf.getInt();
    return new Timetz(time, zone);
  }

  @Override
  public Timetz random(Random r, int size) {
    long time = r.nextLong(0, MAX_TIME);
    int zone = r.nextInt(-43200, 43201);
    return new Timetz(time, zone);
  }

  private static long parseTime(String s) {
    String[] parts = s.split(":");
    if (parts.length < 3) {
      throw new IllegalArgumentException("Invalid time: " + s);
    }
    long hours = Long.parseLong(parts[0]);
    long minutes = Long.parseLong(parts[1]);
    String secPart = parts[2];
    long seconds;
    long micros = 0;
    int dot = secPart.indexOf('.');
    if (dot >= 0) {
      seconds = Long.parseLong(secPart.substring(0, dot));
      String frac = secPart.substring(dot + 1);
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

  private static int findTimezoneStart(String s) {
    for (int i = s.length() - 1; i >= 0; i--) {
      char c = s.charAt(i);
      if (c == '+' || c == '-') {
        if (i > 0 && s.charAt(i - 1) != ':' && s.charAt(i - 1) != '.') {
          return i;
        }
      }
    }
    throw new IllegalArgumentException("No timezone found in: " + s);
  }

  private static int parseTimezone(String tz) {
    char sign = tz.charAt(0);
    String abs = tz.substring(1);
    String[] parts = abs.split(":");
    int hours = Integer.parseInt(parts[0]);
    int minutes = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
    int seconds = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;
    int displayOffset = hours * 3600 + minutes * 60 + seconds;
    if (sign == '-') {
      displayOffset = -displayOffset;
    }
    return -displayOffset; // invert for internal storage
  }

  /** Appends a zero-padded 2-digit integer. */
  private static void pad2(StringBuilder sb, long v) {
    if (v < 10) sb.append('0');
    sb.append(v);
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
}
