package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code macaddr8} values (EUI-64 MAC addresses). */
final class Macaddr8Codec implements Codec<Macaddr8> {

  @Override
  public String name() {
    return "macaddr8";
  }

  @Override
  public int scalarOid() {
    return 774;
  }

  @Override
  public int arrayOid() {
    return 775;
  }

  @Override
  public void encodeInText(StringBuilder sb, Macaddr8 value) {
    value.appendInTextTo(sb);
  }

  @Override
  public Codec.ParsingResult<Macaddr8> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    String[] parts = s.split(":");
    if (parts.length != 8) {
      throw new Codec.DecodingException(input, offset, "Invalid macaddr8: " + s);
    }
    try {
      byte b1 = (byte) Integer.parseInt(parts[0], 16);
      byte b2 = (byte) Integer.parseInt(parts[1], 16);
      byte b3 = (byte) Integer.parseInt(parts[2], 16);
      byte b4 = (byte) Integer.parseInt(parts[3], 16);
      byte b5 = (byte) Integer.parseInt(parts[4], 16);
      byte b6 = (byte) Integer.parseInt(parts[5], 16);
      byte b7 = (byte) Integer.parseInt(parts[6], 16);
      byte b8 = (byte) Integer.parseInt(parts[7], 16);
      return new Codec.ParsingResult<>(
          new Macaddr8(b1, b2, b3, b4, b5, b6, b7, b8), input.length());
    } catch (NumberFormatException e) {
      throw new Codec.DecodingException(input, offset, "Invalid macaddr8 hex: " + s);
    }
  }

  @Override
  public void encodeInBinary(Macaddr8 value, ByteArrayOutputStream out) {
    out.write(value.b1());
    out.write(value.b2());
    out.write(value.b3());
    out.write(value.b4());
    out.write(value.b5());
    out.write(value.b6());
    out.write(value.b7());
    out.write(value.b8());
  }

  @Override
  public Macaddr8 decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    if (length != 8) {
      throw new Codec.DecodingException("Binary macaddr8 must be 8 bytes, got " + length);
    }
    return new Macaddr8(
        buf.get(), buf.get(), buf.get(), buf.get(), buf.get(), buf.get(), buf.get(), buf.get());
  }

  @Override
  public Macaddr8 random(Random r, int size) {
    byte[] b = new byte[8];
    r.nextBytes(b);
    return new Macaddr8(b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]);
  }
}
