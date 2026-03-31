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
  public void encodeInText(StringBuilder sb, Timetz value) {
    value.appendInTextTo(sb);
  }

  @Override
  public Codec.ParsingResult<Timetz> decodeInText(CharSequence input, int offset)
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
    String secPart = parts[2];
    int dot = secPart.indexOf('.');
    if (dot >= 0) {
      long minutes = Long.parseLong(parts[1]);
      long seconds = Long.parseLong(secPart.substring(0, dot));
      long micros = Long.parseLong((secPart.substring(dot + 1) + "000000").substring(0, 6));
      return hours * 3_600_000_000L + minutes * 60_000_000L + seconds * 1_000_000L + micros;
    }
    long minutes = Long.parseLong(parts[1]);
    long seconds = Long.parseLong(secPart);
    return hours * 3_600_000_000L + minutes * 60_000_000L + seconds * 1_000_000L;
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
}
