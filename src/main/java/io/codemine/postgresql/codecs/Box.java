package io.codemine.postgresql.codecs;

/**
 * PostgreSQL {@code box} type. A rectangular box defined by upper-right (x1, y1) and lower-left
 * (x2, y2) corners.
 *
 * <p>PostgreSQL normalizes box coordinates so that (x1, y1) is always the upper-right corner and
 * (x2, y2) is always the lower-left corner. Use {@link #of} to create a normalized box from
 * arbitrary corner coordinates.
 */
public record Box(double x1, double y1, double x2, double y2) {

  /**
   * Creates a normalized box where (x1, y1) is upper-right and (x2, y2) is lower-left, matching
   * PostgreSQL's canonical ordering.
   */
  public static Box of(double x1, double y1, double x2, double y2) {
    return new Box(Math.max(x1, x2), Math.max(y1, y2), Math.min(x1, x2), Math.min(y1, y2));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    sb.append('(');
    sb.append(x1);
    sb.append(',');
    sb.append(y1);
    sb.append("),(");
    sb.append(x2);
    sb.append(',');
    sb.append(y2);
    sb.append(')');
  }
}
