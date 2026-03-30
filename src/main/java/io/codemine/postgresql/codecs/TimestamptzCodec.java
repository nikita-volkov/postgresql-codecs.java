package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

/** Codec for PostgreSQL {@code timestamptz} values, represented as {@link Instant}. */
final class TimestamptzCodec implements Codec<Instant> {

  private static final long PG_EPOCH_UNIX_SECONDS = 946_684_800L;
  private static final long PG_EPOCH_UNIX_MICROS = PG_EPOCH_UNIX_SECONDS * 1_000_000L;

  @Override
  public String name() {
    return "timestamptz";
  }

  @Override
  public int scalarOid() {
    return 1184;
  }

  @Override
  public int arrayOid() {
    return 1185;
  }

  @Override
  public void write(StringBuilder sb, Instant value) {
    long unixMicros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1_000L;
    long epochSecond = Math.floorDiv(unixMicros, 1_000_000L);
    long microOfSecond = Math.floorMod(unixMicros, 1_000_000L);
    LocalDateTime dt = LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);
    pad4(sb, dt.getYear());
    sb.append('-');
    pad2(sb, dt.getMonthValue());
    sb.append('-');
    pad2(sb, dt.getDayOfMonth());
    sb.append(' ');
    pad2(sb, dt.getHour());
    sb.append(':');
    pad2(sb, dt.getMinute());
    sb.append(':');
    pad2(sb, dt.getSecond());
    appendFraction(sb, microOfSecond);
    sb.append("+00");
  }

  @Override
  public Codec.ParsingResult<Instant> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      // Format: YYYY-MM-DD hh:mm:ss[.ffffff][+-hh[:mm[:ss]]]
      int spaceIdx = s.indexOf(' ');
      if (spaceIdx < 0) {
        throw new IllegalArgumentException("Invalid timestamptz: " + s);
      }
      String datePart = s.substring(0, spaceIdx);
      String timePart = s.substring(spaceIdx + 1);

      String[] dateFields = datePart.split("-");
      int year = Integer.parseInt(dateFields[0]);
      int month = Integer.parseInt(dateFields[1]);
      int day = Integer.parseInt(dateFields[2]);

      // Find and strip timezone suffix
      int tzStart = findTimezoneStart(timePart);
      int tzOffset = 0;
      if (tzStart >= 0) {
        tzOffset = parseTimezoneOffset(timePart.substring(tzStart));
        timePart = timePart.substring(0, tzStart);
      }

      String[] timeFields = timePart.split(":");
      int hour = Integer.parseInt(timeFields[0]);
      int minute = Integer.parseInt(timeFields[1]);
      String secStr = timeFields[2];
      int second;
      long microOfSecond = 0;
      int dot = secStr.indexOf('.');
      if (dot >= 0) {
        second = Integer.parseInt(secStr.substring(0, dot));
        String frac = secStr.substring(dot + 1);
        while (frac.length() < 6) {
          frac = frac + "0";
        }
        if (frac.length() > 6) {
          frac = frac.substring(0, 6);
        }
        microOfSecond = Long.parseLong(frac);
      } else {
        second = Integer.parseInt(secStr);
      }

      LocalDateTime dt = LocalDateTime.of(year, month, day, hour, minute, second);
      long epochSecond = dt.toEpochSecond(ZoneOffset.UTC) - tzOffset;
      long unixMicros = epochSecond * 1_000_000L + microOfSecond;
      return new Codec.ParsingResult<>(
          Instant.ofEpochSecond(
              Math.floorDiv(unixMicros, 1_000_000L),
              Math.floorMod(unixMicros, 1_000_000L) * 1_000L),
          input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid timestamptz: " + s);
    }
  }

  @Override
  public void encodeInBinary(Instant value, ByteArrayOutputStream out) {
    long unixMicros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1_000L;
    long v = unixMicros - PG_EPOCH_UNIX_MICROS;
    out.write((int) (v >>> 56) & 0xFF);
    out.write((int) (v >>> 48) & 0xFF);
    out.write((int) (v >>> 40) & 0xFF);
    out.write((int) (v >>> 32) & 0xFF);
    out.write((int) (v >>> 24) & 0xFF);
    out.write((int) (v >>> 16) & 0xFF);
    out.write((int) (v >>> 8) & 0xFF);
    out.write((int) (v & 0xFF));
  }

  @Override
  public Instant decodeInBinary(ByteBuffer buf, int length) {
    long pgMicros = buf.getLong();
    long unixMicros = pgMicros + PG_EPOCH_UNIX_MICROS;
    return Instant.ofEpochSecond(
        Math.floorDiv(unixMicros, 1_000_000L), Math.floorMod(unixMicros, 1_000_000L) * 1_000L);
  }

  @Override
  public Instant random(Random r, int size) {
    if (size == 0) {
      return Instant.ofEpochSecond(PG_EPOCH_UNIX_SECONDS);
    }
    long bound = (long) size * 86_400_000_000L;
    long pgMicros = r.nextLong(-bound, bound + 1);
    long unixMicros = pgMicros + PG_EPOCH_UNIX_MICROS;
    return Instant.ofEpochSecond(
        Math.floorDiv(unixMicros, 1_000_000L), Math.floorMod(unixMicros, 1_000_000L) * 1_000L);
  }

  private static int findTimezoneStart(String s) {
    for (int i = s.length() - 1; i >= 0; i--) {
      char c = s.charAt(i);
      if (c == '+' || c == '-') {
        if (i > 0 && Character.isDigit(s.charAt(i - 1))) {
          return i;
        }
      }
    }
    return -1;
  }

  private static int parseTimezoneOffset(String tz) {
    char sign = tz.charAt(0);
    String abs = tz.substring(1);
    String[] parts = abs.split(":");
    int hours = Integer.parseInt(parts[0]);
    int minutes = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
    int seconds = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;
    int offset = hours * 3600 + minutes * 60 + seconds;
    if (sign == '-') {
      offset = -offset;
    }
    return offset;
  }

  /** Appends a zero-padded 4-digit year. */
  private static void pad4(StringBuilder sb, int v) {
    if (v < 10) sb.append("000");
    else if (v < 100) sb.append("00");
    else if (v < 1000) sb.append('0');
    sb.append(v);
  }

  /** Appends a zero-padded 2-digit integer. */
  private static void pad2(StringBuilder sb, int v) {
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
