package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code varbit} (variable-length bit string) values. */
final class VarbitCodec implements Codec<Bit> {

  @Override
  public String name() {
    return "varbit";
  }

  @Override
  public int scalarOid() {
    return 1562;
  }

  @Override
  public int arrayOid() {
    return 1563;
  }

  @Override
  public void write(StringBuilder sb, Bit value) {
    BitCodec.writeBit(sb, value);
  }

  @Override
  public Codec.ParsingResult<Bit> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    return BitCodec.parseBit(input, offset);
  }

  @Override
  public void encodeInBinary(Bit value, ByteArrayOutputStream out) {
    BitCodec.encodeBitInBinary(value, out);
  }

  @Override
  public Bit decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    return BitCodec.decodeBitInBinary(buf);
  }

  @Override
  public Bit random(Random r, int size) {
    return BitCodec.randomBit(r, size);
  }
}
