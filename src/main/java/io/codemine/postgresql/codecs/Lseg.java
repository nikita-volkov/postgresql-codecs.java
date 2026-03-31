package io.codemine.postgresql.codecs;

/** PostgreSQL {@code lseg} type. A line segment defined by two endpoints. */
public record Lseg(double x1, double y1, double x2, double y2) {

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    sb.append("[(");
    sb.append(x1);
    sb.append(',');
    sb.append(y1);
    sb.append("),(");
    sb.append(x2);
    sb.append(',');
    sb.append(y2);
    sb.append(")]");
  }
}
