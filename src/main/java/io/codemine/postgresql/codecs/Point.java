package io.codemine.postgresql.codecs;

/** PostgreSQL {@code point} type. A point on a plane: (x, y). */
public record Point(double x, double y) {
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    sb.append('(');
    sb.append(x);
    sb.append(',');
    sb.append(y);
    sb.append(')');
  }
}
