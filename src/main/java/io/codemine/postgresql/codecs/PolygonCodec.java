package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Codec for PostgreSQL {@code polygon} values. */
final class PolygonCodec implements Codec<Polygon> {

  @Override
  public String name() {
    return "polygon";
  }

  @Override
  public int scalarOid() {
    return 604;
  }

  @Override
  public int arrayOid() {
    return 1027;
  }

  @Override
  public void encodeInText(StringBuilder sb, Polygon value) {
    value.appendInTextTo(sb);
  }

  @Override
  public Codec.ParsingResult<Polygon> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      // Format: ((x1,y1),(x2,y2),...) — strip outer parens
      if (s.startsWith("(") && s.endsWith(")")) {
        s = s.substring(1, s.length() - 1);
      }

      // Parse comma-separated point list: (x1,y1),(x2,y2),...
      List<Point> points = new ArrayList<>();
      s = s.trim();
      if (!s.isEmpty()) {
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
          char c = s.charAt(i);
          if (c == '(') {
            depth++;
          } else if (c == ')') {
            depth--;
          } else if (c == ',' && depth == 0) {
            points.add(parsePoint(s.substring(start, i).trim()));
            start = i + 1;
          }
        }
        points.add(parsePoint(s.substring(start).trim()));
      }

      return new Codec.ParsingResult<>(new Polygon(points), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid polygon: " + s);
    }
  }

  @Override
  public void encodeInBinary(Polygon value, ByteArrayOutputStream out) {
    int numPoints = value.points().size();
    out.write((numPoints >>> 24) & 0xFF);
    out.write((numPoints >>> 16) & 0xFF);
    out.write((numPoints >>> 8) & 0xFF);
    out.write(numPoints & 0xFF);
    for (Point p : value.points()) {
      writeFloat8(out, p.x());
      writeFloat8(out, p.y());
    }
  }

  @Override
  public Polygon decodeInBinary(ByteBuffer buf, int length) {
    int numPoints = buf.getInt();
    List<Point> points = new ArrayList<>(numPoints);
    for (int i = 0; i < numPoints; i++) {
      points.add(new Point(buf.getDouble(), buf.getDouble()));
    }
    return new Polygon(points);
  }

  @Override
  public Polygon random(Random r, int size) {
    int numPoints = size == 0 ? 1 : r.nextInt(1, Math.min(size, 10) + 1);
    List<Point> points = new ArrayList<>(numPoints);
    for (int i = 0; i < numPoints; i++) {
      points.add(new Point((r.nextDouble() * 2 - 1) * size, (r.nextDouble() * 2 - 1) * size));
    }
    return new Polygon(points);
  }

  private static Point parsePoint(String s) {
    s = s.trim();
    if (s.startsWith("(") && s.endsWith(")")) {
      s = s.substring(1, s.length() - 1);
    }
    int comma = s.indexOf(',');
    if (comma < 0) {
      throw new IllegalArgumentException("No comma in point: " + s);
    }
    double x = Double.parseDouble(s.substring(0, comma).trim());
    double y = Double.parseDouble(s.substring(comma + 1).trim());
    return new Point(x, y);
  }

  private static void writeFloat8(ByteArrayOutputStream out, double value) {
    long bits = Double.doubleToLongBits(value);
    out.write((int) (bits >>> 56) & 0xFF);
    out.write((int) (bits >>> 48) & 0xFF);
    out.write((int) (bits >>> 40) & 0xFF);
    out.write((int) (bits >>> 32) & 0xFF);
    out.write((int) (bits >>> 24) & 0xFF);
    out.write((int) (bits >>> 16) & 0xFF);
    out.write((int) (bits >>> 8) & 0xFF);
    out.write((int) bits & 0xFF);
  }
}
