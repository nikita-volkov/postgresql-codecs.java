package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

/** Codec for PostgreSQL {@code box} values. */
final class BoxCodec implements Codec<Box> {

  @Override
  public String name() {
    return "box";
  }

  @Override
  public int scalarOid() {
    return 603;
  }

  @Override
  public int arrayOid() {
    return 1020;
  }

  @Override
  public Codec<List<Box>> inDim() {
    return new ArrayCodec<>(this, ';');
  }

  @Override
  public void encodeInText(StringBuilder sb, Box value) {
    value.appendInTextTo(sb);
  }

  @Override
  public Codec.ParsingResult<Box> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      // Format: (x1,y1),(x2,y2)
      // Find comma between the two points: ),(
      int midSep = -1;
      int depth = 0;
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        if (c == '(') {
          depth++;
        } else if (c == ')') {
          depth--;
        } else if (c == ',' && depth == 0) {
          midSep = i;
          break;
        }
      }
      if (midSep < 0) {
        throw new IllegalArgumentException("No point separator in: " + s);
      }
      Point p1 = parsePoint(s.substring(0, midSep));
      Point p2 = parsePoint(s.substring(midSep + 1));
      return new Codec.ParsingResult<>(new Box(p1.x(), p1.y(), p2.x(), p2.y()), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid box: " + s);
    }
  }

  @Override
  public void encodeInBinary(Box value, ByteArrayOutputStream out) {
    writeFloat8(out, value.x1());
    writeFloat8(out, value.y1());
    writeFloat8(out, value.x2());
    writeFloat8(out, value.y2());
  }

  @Override
  public Box decodeInBinary(ByteBuffer buf, int length) {
    return new Box(buf.getDouble(), buf.getDouble(), buf.getDouble(), buf.getDouble());
  }

  @Override
  public Box random(Random r, int size) {
    if (size == 0) {
      return new Box(0.0, 0.0, 0.0, 0.0);
    }
    return Box.of(
        (r.nextDouble() * 2 - 1) * size,
        (r.nextDouble() * 2 - 1) * size,
        (r.nextDouble() * 2 - 1) * size,
        (r.nextDouble() * 2 - 1) * size);
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
