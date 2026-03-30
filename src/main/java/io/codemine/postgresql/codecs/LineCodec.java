package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code line} values ({A,B,C} where Ax + By + C = 0). */
final class LineCodec implements Codec<Line> {

  @Override
  public String name() {
    return "line";
  }

  @Override
  public int scalarOid() {
    return 628;
  }

  @Override
  public int arrayOid() {
    return 629;
  }

  @Override
  public void write(StringBuilder sb, Line value) {
    sb.append('{');
    sb.append(Double.toString(value.a()));
    sb.append(',');
    sb.append(Double.toString(value.b()));
    sb.append(',');
    sb.append(Double.toString(value.c()));
    sb.append('}');
  }

  @Override
  public Codec.ParsingResult<Line> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      if (s.startsWith("{") && s.endsWith("}")) {
        s = s.substring(1, s.length() - 1);
      }
      String[] parts = s.split(",");
      double a = Double.parseDouble(parts[0].trim());
      double b = Double.parseDouble(parts[1].trim());
      double c = Double.parseDouble(parts[2].trim());
      return new Codec.ParsingResult<>(new Line(a, b, c), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid line: " + s);
    }
  }

  @Override
  public void encodeInBinary(Line value, ByteArrayOutputStream out) {
    writeFloat8(out, value.a());
    writeFloat8(out, value.b());
    writeFloat8(out, value.c());
  }

  @Override
  public Line decodeInBinary(ByteBuffer buf, int length) {
    return new Line(buf.getDouble(), buf.getDouble(), buf.getDouble());
  }

  @Override
  public Line random(Random r, int size) {
    if (size == 0) return new Line(1.0, 0.0, 0.0);
    double a = (r.nextDouble() * 2 - 1) * size;
    double b = (r.nextDouble() * 2 - 1) * size;
    if (a == 0 && b == 0) b = 1.0;
    double c = (r.nextDouble() * 2 - 1) * size;
    return new Line(a, b, c);
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
