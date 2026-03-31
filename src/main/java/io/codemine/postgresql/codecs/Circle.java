package io.codemine.postgresql.codecs;

/**
 * PostgreSQL {@code circle} type. A circle defined by center point (x, y) and radius r.
 *
 * @param x the x-coordinate of the center
 * @param y the y-coordinate of the center
 * @param r the radius (non-negative)
 */
public record Circle(double x, double y, double r) {

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    sb.append("<(");
    sb.append(x);
    sb.append(',');
    sb.append(y);
    sb.append("),");
    sb.append(r);
    sb.append('>');
  }
}
