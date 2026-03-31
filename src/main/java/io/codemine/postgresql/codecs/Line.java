package io.codemine.postgresql.codecs;

/** PostgreSQL {@code line} type. Represents the line Ax + By + C = 0. */
public record Line(double a, double b, double c) {

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    sb.append('{');
    sb.append(a);
    sb.append(',');
    sb.append(b);
    sb.append(',');
    sb.append(c);
    sb.append('}');
  }
}
