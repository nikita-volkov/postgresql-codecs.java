package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.Random;

/** Codec for PostgreSQL {@code bytea} values. */
final class ByteaCodec implements Codec<Bytea> {

  private static final HexFormat HEX = HexFormat.of();

  @Override
  public String name() {
    return "bytea";
  }

  @Override
  public int scalarOid() {
    return 17;
  }

  @Override
  public int arrayOid() {
    return 1001;
  }

  @Override
  public void encodeInText(StringBuilder sb, Bytea value) {
    value.appendInTextTo(sb);
  }

  @Override
  public Codec.ParsingResult<Bytea> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    if (!s.startsWith("\\x")) {
      throw new Codec.DecodingException(input, offset, "Invalid bytea: missing \\x prefix");
    }
    try {
      byte[] bytes = HEX.parseHex(s.substring(2));
      return new Codec.ParsingResult<>(new Bytea(bytes), input.length());
    } catch (IllegalArgumentException e) {
      throw new Codec.DecodingException(input, offset, "Invalid bytea hex: " + e.getMessage());
    }
  }

  @Override
  public void encodeInBinary(Bytea value, ByteArrayOutputStream out) {
    out.write(value.bytes(), 0, value.bytes().length);
  }

  @Override
  public Bytea decodeInBinary(ByteBuffer buf, int length) {
    byte[] bytes = new byte[length];
    buf.get(bytes);
    return new Bytea(bytes);
  }

  @Override
  public Bytea random(Random r, int size) {
    byte[] bytes = new byte[size];
    r.nextBytes(bytes);
    return new Bytea(bytes);
  }
}
