package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

/** Codec for PostgreSQL {@code timestamp} values, represented as {@link LocalDateTime}. */
final class TimestampCodec implements Codec<LocalDateTime> {

  // Unix epoch seconds at 2000-01-01T00:00:00 UTC
  static final long PG_EPOCH_UNIX_SECONDS = 946_684_800L;
  static final long PG_EPOCH_UNIX_MICROS = PG_EPOCH_UNIX_SECONDS * 1_000_000L;

  @Override
  public String name() {
    return "timestamp";
  }

  @Override
  public int scalarOid() {
    return 1114;
  }

  @Override
  public int arrayOid() {
    return 1115;
  }

  @Override
  public void write(StringBuilder sb, LocalDateTime value) {
    long pgMicros = toPgMicros(value);
    writeTimestamp(sb, pgMicros);
  }

  @Override
  public Codec.ParsingResult<LocalDateTime> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      long pgMicros = parseTimestamp(s);
      return new Codec.ParsingResult<>(fromPgMicros(pgMicros), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid timestamp: " + s);
    }
  }

  @Override
  public void encodeInBinary(LocalDateTime value, ByteArrayOutputStream out) {
    long v = toPgMicros(value);
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
  public LocalDateTime decodeInBinary(ByteBuffer buf, int length) {
    long pgMicros = buf.getLong();
    return fromPgMicros(pgMicros);
  }

  @Override
  public LocalDateTime random(Random r, int size) {
    if (size == 0) return LocalDateTime.of(2000, 1, 1, 0, 0, 0);
    long bound = (long) size * 86_400_000_000L;
    long pgMicros = r.nextLong(-bound, bound + 1);
    // Truncate to microseconds (which is what PG stores)
    return fromPgMicros(pgMicros);
  }

  /** Converts a {@link LocalDateTime} to PG microseconds from 2000-01-01. */
  static long toPgMicros(LocalDateTime dt) {
    long epochSecond = dt.toEpochSecond(ZoneOffset.UTC);
    long nanoOfSecond = dt.getNano();
    long unixMicros = epochSecond * 1_000_000L + nanoOfSecond / 1_000L;
    return unixMicros - PG_EPOCH_UNIX_MICROS;
  }

  /** Converts PG microseconds from 2000-01-01 to a {@link LocalDateTime}. */
  static LocalDateTime fromPgMicros(long pgMicros) {
    long unixMicros = pgMicros + PG_EPOCH_UNIX_MICROS;
    long epochSecond = Math.floorDiv(unixMicros, 1_000_000L);
    long microOfSecond = Math.floorMod(unixMicros, 1_000_000L);
    return LocalDateTime.ofEpochSecond(epochSecond, (int) (microOfSecond * 1_000L), ZoneOffset.UTC);
  }

  /** Writes PG microseconds from 2000-01-01 as text timestamp. */
  static void writeTimestamp(StringBuilder sb, long pgMicros) {
    LocalDateTime dt = fromPgMicros(pgMicros);
    sb.append(
        String.format(
            "%04d-%02d-%02d %02d:%02d:%02d",
            dt.getYear(),
            dt.getMonthValue(),
            dt.getDayOfMonth(),
            dt.getHour(),
            dt.getMinute(),
            dt.getSecond()));
    long microOfSecond = dt.getNano() / 1_000L;
    TimeCodec.appendFraction(sb, microOfSecond);
  }

  /** Parses a timestamp string and returns PG microseconds from 2000-01-01. */
  static long parseTimestamp(String s) {
    // Format: YYYY-MM-DD hh:mm:ss[.ffffff][±tz]
    int spaceIdx = s.indexOf(' ');
    if (spaceIdx < 0) {
      throw new IllegalArgumentException("Invalid timestamp: " + s);
    }
    String datePart = s.substring(0, spaceIdx);
    String timePart = s.substring(spaceIdx + 1);

    String[] dateFields = datePart.split("-");
    int year = Integer.parseInt(dateFields[0]);
    int month = Integer.parseInt(dateFields[1]);
    int day = Integer.parseInt(dateFields[2]);

    // Strip timezone if present
    int tzOffset = 0;
    int tzStart = findTimezoneStart(timePart);
    if (tzStart >= 0) {
      String tzPart = timePart.substring(tzStart);
      timePart = timePart.substring(0, tzStart);
      tzOffset = parseTimezoneOffset(tzPart);
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
      while (frac.length() < 6) frac = frac + "0";
      if (frac.length() > 6) frac = frac.substring(0, 6);
      microOfSecond = Long.parseLong(frac);
    } else {
      second = Integer.parseInt(secStr);
    }

    LocalDateTime dt = LocalDateTime.of(year, month, day, hour, minute, second);
    long epochSecond = dt.toEpochSecond(ZoneOffset.UTC) - tzOffset;
    long unixMicros = epochSecond * 1_000_000L + microOfSecond;
    return unixMicros - PG_EPOCH_UNIX_MICROS;
  }

  static int findTimezoneStart(String s) {
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

  static int parseTimezoneOffset(String tz) {
    char sign = tz.charAt(0);
    String abs = tz.substring(1);
    String[] parts = abs.split(":");
    int hours = Integer.parseInt(parts[0]);
    int minutes = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
    int seconds = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;
    int offset = hours * 3600 + minutes * 60 + seconds;
    if (sign == '-') offset = -offset;
    return offset;
  }
}
