package io.codemine.postgresql.codecs;

import java.util.List;

/**
 * PostgreSQL {@code polygon} type. A closed geometric shape defined by a list of vertices.
 *
 * @param points the vertices of the polygon
 */
public record Polygon(List<Point> points) {

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    sb.append('(');
    for (int i = 0; i < points.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      points.get(i).appendInTextTo(sb);
    }
    sb.append(')');
  }
}
