package io.codemine.postgresql.codecs;

/**
 * PostgreSQL {@code interval} type. A time span with separate month, day, and microsecond
 * components.
 *
 * @param time time component in microseconds
 * @param day day component
 * @param month month component (may be decomposed into years and months for display)
 */
public record Interval(long time, int day, int month) {

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    int months = month;
    int years = months / 12;
    int mons = months % 12;
    int days = day;

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
    if (time != 0 || !hasParts) {
      if (hasParts) {
        sb.append(' ');
      }
      writeTime(sb, time);
    }
  }

  private static void writeTime(StringBuilder sb, long timeMicros) {
    if (timeMicros < 0) {
      sb.append('-');
      timeMicros = -timeMicros;
    }
    long hours = timeMicros / 3_600_000_000L;
    timeMicros %= 3_600_000_000L;

    pad2(sb, hours);
    sb.append(':');

    long minutes = timeMicros / 60_000_000L;
    timeMicros %= 60_000_000L;
    pad2(sb, minutes);
    sb.append(':');

    long seconds = timeMicros / 1_000_000L;
    long frac = timeMicros % 1_000_000L;
    pad2(sb, seconds);
    if (frac > 0) {
      appendFraction(sb, frac);
    }
  }

  private static void pad2(StringBuilder sb, long v) {
    if (v < 10) {
      sb.append('0');
    }
    sb.append(v);
  }

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
    while (sb.charAt(len - 1) == '0') {
      len--;
    }
    sb.setLength(len);
  }
}
