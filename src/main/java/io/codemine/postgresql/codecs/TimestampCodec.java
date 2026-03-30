package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

/** Codec for PostgreSQL {@code timestamp} values, represented as {@link LocalDateTime}. */
final class TimestampCodec implements Codec<LocalDateTime> {

  private static final long PG_EPOCH_UNIX_SECONDS = 946_684_800L;
  private static final long PG_EPOCH_UNIX_MICROS = PG_EPOCH_UNIX_SECONDS * 1_000_000L;

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
    pad4(sb, value.getYear());
    sb.append('-');
    pad2(sb, value.getMonthValue());
    sb.append('-');
    pad2(sb, value.getDayOfMonth());
    sb.append(' ');
    pad2(sb, value.getHour());
    sb.append(':');
    pad2(sb, value.getMinute());
    sb.append(':');
    pad2(sb, value.getSecond());
    appendFraction(sb, value.getNano() / 1_000L);
  }

  @Override
  public Codec.ParsingResult<LocalDateTime> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      // Format: YYYY-MM-DD hh:mm:ss[.ffffff]
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

      return new Codec.ParsingResult<>(
          LocalDateTime.of(year, month, day, hour, minute, second, (int) (microOfSecond * 1_000L)),
          input.length());
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
    if (size == 0) {
      return LocalDateTime.of(2000, 1, 1, 0, 0, 0);
    }
    long bound = (long) size * 86_400_000_000L;
    long pgMicros = r.nextLong(-bound, bound + 1);
    return fromPgMicros(pgMicros);
  }

  private static long toPgMicros(LocalDateTime dt) {
    long epochSecond = dt.toEpochSecond(ZoneOffset.UTC);
    long nanoOfSecond = dt.getNano();
    long unixMicros = epochSecond * 1_000_000L + nanoOfSecond / 1_000L;
    return unixMicros - PG_EPOCH_UNIX_MICROS;
  }

  private static LocalDateTime fromPgMicros(long pgMicros) {
    long unixMicros = pgMicros + PG_EPOCH_UNIX_MICROS;
    long epochSecond = Math.floorDiv(unixMicros, 1_000_000L);
    long microOfSecond = Math.floorMod(unixMicros, 1_000_000L);
    return LocalDateTime.ofEpochSecond(epochSecond, (int) (microOfSecond * 1_000L), ZoneOffset.UTC);
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
