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
    writeBit(sb, value);
  }

  @Override
  public Codec.ParsingResult<Bit> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    return parseBit(input, offset);
  }

  @Override
  public void encodeInBinary(Bit value, ByteArrayOutputStream out) {
    encodeBitInBinary(value, out);
  }

  @Override
  public Bit decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    return decodeBitInBinary(buf);
  }

  @Override
  public Bit random(Random r, int size) {
    return randomBit(r, size);
  }

  // -----------------------------------------------------------------------
  // Shared implementation reused by VarbitCodec
  // -----------------------------------------------------------------------

  static void writeBit(StringBuilder sb, Bit value) {
    for (int i = 0; i < value.length(); i++) {
      int byteIndex = i / 8;
      int bitIndex = 7 - (i % 8);
      sb.append((value.data()[byteIndex] >> bitIndex) & 1);
    }
  }

  static Codec.ParsingResult<Bit> parseBit(CharSequence input, int offset)
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

  static void encodeBitInBinary(Bit value, ByteArrayOutputStream out) {
    int len = value.length();
    out.write((len >>> 24) & 0xFF);
    out.write((len >>> 16) & 0xFF);
    out.write((len >>> 8) & 0xFF);
    out.write(len & 0xFF);
    out.writeBytes(value.data());
  }

  static Bit decodeBitInBinary(ByteBuffer buf) {
    int bitCount = buf.getInt();
    int numBytes = (bitCount + 7) / 8;
    byte[] data = new byte[numBytes];
    buf.get(data);
    return new Bit(bitCount, data);
  }

  static Bit randomBit(Random r, int size) {
    int len = size == 0 ? 1 : r.nextInt(1, size + 1);
    int numBytes = (len + 7) / 8;
    byte[] data = new byte[numBytes];
    r.nextBytes(data);
    // Zero out padding bits in the last byte
    int padding = numBytes * 8 - len;
    if (padding > 0) {
      data[numBytes - 1] &= (byte) (0xFF << padding);
    }
    return new Bit(len, data);
  }
}
