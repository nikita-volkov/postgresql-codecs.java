package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code circle} values. */
final class CircleCodec implements Codec<Circle> {

  @Override
  public String name() {
    return "circle";
  }

  @Override
  public int scalarOid() {
    return 718;
  }

  @Override
  public int arrayOid() {
    return 719;
  }

  @Override
  public void encodeInText(StringBuilder sb, Circle value) {
    value.appendInTextTo(sb);
  }

  @Override
  public Codec.ParsingResult<Circle> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      // Format: <(x,y),r>
      if (s.startsWith("<") && s.endsWith(">")) {
        s = s.substring(1, s.length() - 1);
      }
      // Now: (x,y),r — find the closing paren of the point
      int closeParen = s.indexOf(')');
      String pointStr = s.substring(0, closeParen + 1);
      String radiusStr = s.substring(closeParen + 2).trim();
      Point center = parsePoint(pointStr);
      double radius = Double.parseDouble(radiusStr);
      return new Codec.ParsingResult<>(new Circle(center.x(), center.y(), radius), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid circle: " + s);
    }
  }

  @Override
  public void encodeInBinary(Circle value, ByteArrayOutputStream out) {
    writeFloat8(out, value.x());
    writeFloat8(out, value.y());
    writeFloat8(out, value.r());
  }

  @Override
  public Circle decodeInBinary(ByteBuffer buf, int length) {
    return new Circle(buf.getDouble(), buf.getDouble(), buf.getDouble());
  }

  @Override
  public Circle random(Random r, int size) {
    if (size == 0) {
      return new Circle(0.0, 0.0, 0.0);
    }
    return new Circle(
        (r.nextDouble() * 2 - 1) * size, (r.nextDouble() * 2 - 1) * size, r.nextDouble() * size);
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
