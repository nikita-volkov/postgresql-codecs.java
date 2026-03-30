package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Codec for PostgreSQL {@code path} values. */
final class PathCodec implements Codec<Path> {

  @Override
  public String name() {
    return "path";
  }

  @Override
  public int scalarOid() {
    return 602;
  }

  @Override
  public int arrayOid() {
    return 1019;
  }

  @Override
  public void write(StringBuilder sb, Path value) {
    sb.append(value.closed() ? '(' : '[');
    for (int i = 0; i < value.points().size(); i++) {
      if (i > 0) sb.append(',');
      Point p = value.points().get(i);
      sb.append('(');
      sb.append(Double.toString(p.x()));
      sb.append(',');
      sb.append(Double.toString(p.y()));
      sb.append(')');
    }
    sb.append(value.closed() ? ')' : ']');
  }

  @Override
  public Codec.ParsingResult<Path> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      boolean closed;
      if (s.startsWith("[")) {
        closed = false;
        s = s.substring(1, s.length() - 1);
      } else if (s.startsWith("(")) {
        closed = true;
        s = s.substring(1, s.length() - 1);
      } else {
        throw new IllegalArgumentException("Path must start with '(' or '['");
      }

      List<Point> points = parsePoints(s);
      return new Codec.ParsingResult<>(new Path(closed, points), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid path: " + s);
    }
  }

  @Override
  public void encodeInBinary(Path value, ByteArrayOutputStream out) {
    out.write(value.closed() ? 1 : 0);
    int numPoints = value.points().size();
    out.write((numPoints >>> 24) & 0xFF);
    out.write((numPoints >>> 16) & 0xFF);
    out.write((numPoints >>> 8) & 0xFF);
    out.write(numPoints & 0xFF);
    for (Point p : value.points()) {
      PointCodec.writeFloat8(out, p.x());
      PointCodec.writeFloat8(out, p.y());
    }
  }

  @Override
  public Path decodeInBinary(ByteBuffer buf, int length) {
    boolean closed = buf.get() != 0;
    int numPoints = buf.getInt();
    List<Point> points = new ArrayList<>(numPoints);
    for (int i = 0; i < numPoints; i++) {
      points.add(new Point(buf.getDouble(), buf.getDouble()));
    }
    return new Path(closed, points);
  }

  @Override
  public Path random(Random r, int size) {
    boolean closed = r.nextBoolean();
    int numPoints = size == 0 ? 1 : r.nextInt(1, Math.min(size, 10) + 1);
    List<Point> points = new ArrayList<>(numPoints);
    for (int i = 0; i < numPoints; i++) {
      points.add(new Point(PointCodec.finiteDouble(r, size), PointCodec.finiteDouble(r, size)));
    }
    return new Path(closed, points);
  }

  /**
   * Parses a comma-separated list of points: "(x1,y1),(x2,y2),..." The input should NOT include the
   * outer brackets.
   */
  static List<Point> parsePoints(String s) {
    List<Point> points = new ArrayList<>();
    s = s.trim();
    if (s.isEmpty()) return points;

    int depth = 0;
    int start = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '(') depth++;
      else if (c == ')') depth--;
      else if (c == ',' && depth == 0) {
        points.add(PointCodec.parsePoint(s.substring(start, i).trim()));
        start = i + 1;
      }
    }
    points.add(PointCodec.parsePoint(s.substring(start).trim()));
    return points;
  }
}
