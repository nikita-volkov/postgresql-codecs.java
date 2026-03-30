package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code bit} (fixed-length bit string) values. */
final class BitCodec implements Codec<Bit> {

  @Override
  public String name() {
    return "bit";
  }

  @Override
  public int scalarOid() {
    return 1560;
  }

  @Override
  public int arrayOid() {
    return 1561;
  }

  @Override
  public void write(StringBuilder sb, Bit value) {
    for (int i = 0; i < value.length(); i++) {
      int byteIndex = i / 8;
      int bitIndex = 7 - (i % 8);
      sb.append((value.data()[byteIndex] >> bitIndex) & 1);
    }
  }

  @Override
  public Codec.ParsingResult<Bit> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    int length = s.length();
    int numBytes = (length + 7) / 8;
    byte[] data = new byte[numBytes];
    for (int i = 0; i < length; i++) {
      char c = s.charAt(i);
      if (c != '0' && c != '1') {
        throw new Codec.DecodingException(input, offset, "Invalid bit character: " + c);
      }
      if (c == '1') {
        int byteIndex = i / 8;
        int bitIndex = 7 - (i % 8);
        data[byteIndex] |= (byte) (1 << bitIndex);
      }
    }
    return new Codec.ParsingResult<>(new Bit(length, data), input.length());
  }

  @Override
  public void encodeInBinary(Bit value, ByteArrayOutputStream out) {
    int len = value.length();
    out.write((len >>> 24) & 0xFF);
    out.write((len >>> 16) & 0xFF);
    out.write((len >>> 8) & 0xFF);
    out.write(len & 0xFF);
    out.writeBytes(value.data());
  }

  @Override
  public Bit decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    int bitCount = buf.getInt();
    int numBytes = (bitCount + 7) / 8;
    byte[] data = new byte[numBytes];
    buf.get(data);
    return new Bit(bitCount, data);
  }

  @Override
  public Bit random(Random r, int size) {
    // Unqualified "bit" in PostgreSQL means bit(1). Generate only 1-bit values so that
    // the cast "SELECT $1::bit" does not truncate the value.
    byte val = r.nextBoolean() ? (byte) 0x80 : (byte) 0x00;
    return new Bit(1, new byte[] {val});
  }
}
