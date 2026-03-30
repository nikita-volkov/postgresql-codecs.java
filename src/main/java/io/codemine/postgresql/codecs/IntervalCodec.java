package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code interval} values. */
final class IntervalCodec implements Codec<Interval> {

  @Override
  public String name() {
    return "interval";
  }

  @Override
  public int scalarOid() {
    return 1186;
  }

  @Override
  public int arrayOid() {
    return 1187;
  }

  @Override
  public void write(StringBuilder sb, Interval value) {
    int months = value.month();
    int years = months / 12;
    int mons = months % 12;
    int days = value.day();

    boolean hasParts = false;

    if (years != 0) {
      sb.append(years).append(years == 1 ? " year" : " years");
      hasParts = true;
    }
    if (mons != 0) {
      if (hasParts) {
        sb.append(' ');
      }
      sb.append(mons).append(mons == 1 ? " mon" : " mons");
      hasParts = true;
    }
    if (days != 0) {
      if (hasParts) {
        sb.append(' ');
      }
      sb.append(days).append(days == 1 ? " day" : " days");
      hasParts = true;
    }
    long time = value.time();
    if (time != 0 || !hasParts) {
      if (hasParts) {
        sb.append(' ');
      }
      writeIntervalTime(sb, time);
    }
  }

  @Override
  public Codec.ParsingResult<Interval> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      Interval interval = parseInterval(s);
      return new Codec.ParsingResult<>(interval, input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid interval: " + s);
    }
  }

  @Override
  public void encodeInBinary(Interval value, ByteArrayOutputStream out) {
    long time = value.time();
    out.write((int) (time >>> 56) & 0xFF);
    out.write((int) (time >>> 48) & 0xFF);
    out.write((int) (time >>> 40) & 0xFF);
    out.write((int) (time >>> 32) & 0xFF);
    out.write((int) (time >>> 24) & 0xFF);
    out.write((int) (time >>> 16) & 0xFF);
    out.write((int) (time >>> 8) & 0xFF);
    out.write((int) (time & 0xFF));
    int day = value.day();
    out.write((day >>> 24) & 0xFF);
    out.write((day >>> 16) & 0xFF);
    out.write((day >>> 8) & 0xFF);
    out.write(day & 0xFF);
    int month = value.month();
    out.write((month >>> 24) & 0xFF);
    out.write((month >>> 16) & 0xFF);
    out.write((month >>> 8) & 0xFF);
    out.write(month & 0xFF);
  }

  @Override
  public Interval decodeInBinary(ByteBuffer buf, int length) {
    long time = buf.getLong();
    int day = buf.getInt();
    int month = buf.getInt();
    return new Interval(time, day, month);
  }

  @Override
  public Interval random(Random r, int size) {
    if (size == 0) {
      return new Interval(0, 0, 0);
    }
    long timeBound = (long) size * 3_600_000_000L;
    long time = r.nextLong(-timeBound, timeBound + 1);
    int day = r.nextInt(2 * size + 1) - size;
    int month = r.nextInt(2 * size + 1) - size;
    return new Interval(time, day, month);
  }

  /** Writes the time component of an interval (may be negative). */
  private static void writeIntervalTime(StringBuilder sb, long timeMicros) {
    if (timeMicros < 0) {
      sb.append('-');
      timeMicros = -timeMicros;
    }
    long hours = timeMicros / 3_600_000_000L;
    timeMicros %= 3_600_000_000L;
    long minutes = timeMicros / 60_000_000L;
    timeMicros %= 60_000_000L;
    long seconds = timeMicros / 1_000_000L;
    long frac = timeMicros % 1_000_000L;

    pad2(sb, hours);
    sb.append(':');
    pad2(sb, minutes);
    sb.append(':');
    pad2(sb, seconds);
    if (frac > 0) {
      appendFraction(sb, frac);
    }
  }

  /** Parses a PG interval output string. */
  private static Interval parseInterval(String s) {
    int years = 0;
    int mons = 0;
    int days = 0;
    long timeMicros = 0;

    String[] tokens = s.split("\\s+");
    int i = 0;
    while (i < tokens.length) {
      String token = tokens[i];

      // Check if this token is a time value (contains ':')
      if (token.contains(":")) {
        timeMicros = parseIntervalTime(token);
        i++;
        continue;
      }

      // Try to parse as number + unit
      if (i + 1 < tokens.length) {
        String unit = tokens[i + 1].toLowerCase();
        int num = Integer.parseInt(token);
        switch (unit) {
          case "year", "years" -> years = num;
          case "mon", "mons" -> mons = num;
          case "day", "days" -> days = num;
          default -> throw new IllegalArgumentException("Unknown interval unit: " + unit);
        }
        i += 2;
      } else {
        // Might be a standalone time token
        if (token.contains(":")) {
          timeMicros = parseIntervalTime(token);
        }
        i++;
      }
    }

    int totalMonths = years * 12 + mons;
    return new Interval(timeMicros, days, totalMonths);
  }

  /** Parses a time component like "04:05:06.789" or "-04:05:06". */
  private static long parseIntervalTime(String s) {
    boolean negative = s.startsWith("-");
    if (negative) {
      s = s.substring(1);
    }
    String[] parts = s.split(":");
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
    long total = hours * 3_600_000_000L + minutes * 60_000_000L + seconds * 1_000_000L + micros;
    return negative ? -total : total;
  }

  /** Appends a zero-padded 2-digit integer (hours may exceed 99 for large intervals). */
  private static void pad2(StringBuilder sb, long v) {
    if (v < 10) sb.append('0');
    sb.append(v);
  }

  /** Appends fractional seconds (1-6 digits, trailing zeros stripped). */
  private static void appendFraction(StringBuilder sb, long micros) {
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
