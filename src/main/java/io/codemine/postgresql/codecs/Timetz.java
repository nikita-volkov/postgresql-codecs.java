package io.codemine.postgresql.codecs;

/**
 * PostgreSQL {@code timetz} type. Time of day with time zone.
 *
 * @param time microseconds from midnight (0 to 86400000000)
 * @param zone timezone offset in seconds with <b>inverted sign</b> per PostgreSQL convention: UTC+1
 *     is stored as {@code -3600}
 */
public record Timetz(long time, int zone) {
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    // Write hh:mm:ss[.ffffff]
    long total = time;
    long hours = total / 3_600_000_000L;
    total %= 3_600_000_000L;
    pad2(sb, hours);
    sb.append(':');

    long minutes = total / 60_000_000L;
    total %= 60_000_000L;
    pad2(sb, minutes);
    sb.append(':');

    long seconds = total / 1_000_000L;
    long frac = total % 1_000_000L;
    pad2(sb, seconds);
    appendFraction(sb, frac);

    // Write timezone: internal zone has inverted sign (negative = UTC+)
    int displayOffset = -zone;
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

  /** Appends a zero-padded 2-digit integer. */
  private static void pad2(StringBuilder sb, long v) {
    if (v < 10) {
      sb.append('0');
    }
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
      while (sb.charAt(len - 1) == '0') {
        len--;
      }
      sb.setLength(len);
    }
  }
}
