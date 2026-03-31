package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code macaddr} values. */
final class MacaddrCodec implements Codec<Macaddr> {
  @Override
  public String name() {
    return "macaddr";
  }

  @Override
  public int scalarOid() {
    return 829;
  }

  @Override
  public int arrayOid() {
    return 1040;
  }

  @Override
  public void encodeInText(StringBuilder sb, Macaddr value) {
    value.appendInTextTo(sb);
  }

  @Override
  public Codec.ParsingResult<Macaddr> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    // Format: xx:xx:xx:xx:xx:xx
    String s = input.subSequence(offset, input.length()).toString().trim();
    String[] parts = s.split(":");
    if (parts.length != 6) {
      throw new Codec.DecodingException(input, offset, "Invalid macaddr: " + s);
    }
    try {
      byte b1 = (byte) Integer.parseInt(parts[0], 16);
      byte b2 = (byte) Integer.parseInt(parts[1], 16);
      byte b3 = (byte) Integer.parseInt(parts[2], 16);
      byte b4 = (byte) Integer.parseInt(parts[3], 16);
      byte b5 = (byte) Integer.parseInt(parts[4], 16);
      byte b6 = (byte) Integer.parseInt(parts[5], 16);
      return new Codec.ParsingResult<>(new Macaddr(b1, b2, b3, b4, b5, b6), input.length());
    } catch (NumberFormatException e) {
      throw new Codec.DecodingException(input, offset, "Invalid macaddr hex: " + s);
    }
  }

  @Override
  public void encodeInBinary(Macaddr value, ByteArrayOutputStream out) {
    out.write(value.b1());
    out.write(value.b2());
    out.write(value.b3());
    out.write(value.b4());
    out.write(value.b5());
    out.write(value.b6());
  }

  @Override
  public Macaddr decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    if (length != 6) {
      throw new Codec.DecodingException("Binary macaddr must be 6 bytes, got " + length);
    }
    return new Macaddr(buf.get(), buf.get(), buf.get(), buf.get(), buf.get(), buf.get());
  }

  @Override
  public Macaddr random(Random r, int size) {
    byte[] b = new byte[6];
    r.nextBytes(b);
    return new Macaddr(b[0], b[1], b[2], b[3], b[4], b[5]);
  }
}
