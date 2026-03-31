package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code lseg} values (line segment). */
final class LsegCodec implements Codec<Lseg> {

  @Override
  public String name() {
    return "lseg";
  }

  @Override
  public int scalarOid() {
    return 601;
  }

  @Override
  public int arrayOid() {
    return 1018;
  }

  @Override
  public void encodeInText(StringBuilder sb, Lseg value) {
    value.appendInTextTo(sb);
  }

  @Override
  public Codec.ParsingResult<Lseg> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      // Format: [(x1,y1),(x2,y2)]
      if (s.startsWith("[")) {
        s = s.substring(1);
      }
      if (s.endsWith("]")) {
        s = s.substring(0, s.length() - 1);
      }
      s = s.trim();

      // Find the comma between the two point literals: ),(
      int midComma = -1;
      int depth = 0;
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        if (c == '(') {
          depth++;
        } else if (c == ')') {
          depth--;
        } else if (c == ',' && depth == 0) {
          midComma = i;
          break;
        }
      }
      if (midComma < 0) {
        throw new IllegalArgumentException("No point separator in: " + s);
      }

      Point p1 = parsePoint(s.substring(0, midComma));
      Point p2 = parsePoint(s.substring(midComma + 1));

      return new Codec.ParsingResult<>(new Lseg(p1.x(), p1.y(), p2.x(), p2.y()), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid lseg: " + s);
    }
  }

  @Override
  public void encodeInBinary(Lseg value, ByteArrayOutputStream out) {
    writeFloat8(out, value.x1());
    writeFloat8(out, value.y1());
    writeFloat8(out, value.x2());
    writeFloat8(out, value.y2());
  }

  @Override
  public Lseg decodeInBinary(ByteBuffer buf, int length) {
    return new Lseg(buf.getDouble(), buf.getDouble(), buf.getDouble(), buf.getDouble());
  }

  @Override
  public Lseg random(Random r, int size) {
    if (size == 0) {
      return new Lseg(0.0, 0.0, 0.0, 0.0);
    }
    return new Lseg(
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
