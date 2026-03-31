package io.codemine.postgresql.codecs;

import java.util.List;

/**
 * PostgreSQL {@code path} type. A geometric path consisting of a list of points that may be open or
 * closed.
 *
 * @param closed {@code true} for a closed path, {@code false} for an open path
 * @param points the vertices of the path
 */
public record Path(boolean closed, List<Point> points) {

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    sb.append(closed ? '(' : '[');
    for (int i = 0; i < points.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      points.get(i).appendInTextTo(sb);
    }
    sb.append(closed ? ')' : ']');
  }
}
