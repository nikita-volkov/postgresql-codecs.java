package io.pgenie.postgresqlcodecs.types;

import io.pgenie.postgresqlcodecs.codecs.Codec;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * PostgreSQL {@code macaddr} type. MAC (Media Access Control) address.
 *
 * <p>Represents a 6-byte MAC address stored as six individual bytes. The canonical text format is
 * {@code xx:xx:xx:xx:xx:xx} in lower-case hexadecimal.
 *
 * <p>Port of the Haskell {@code PostgresqlTypes.Macaddr} type.
 */
public record Macaddr(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6) {

  /** Generates a random {@code Macaddr} covering all 6-byte combinations. */
  public static Macaddr generate(Random r) {
    byte[] b = new byte[6];
    r.nextBytes(b);
    return new Macaddr(b[0], b[1], b[2], b[3], b[4], b[5]);
  }

  @Override
  public String toString() {
    return String.format(
        "%02x:%02x:%02x:%02x:%02x:%02x",
        b1 & 0xff, b2 & 0xff, b3 & 0xff, b4 & 0xff, b5 & 0xff, b6 & 0xff);
  }

  public static final Codec<Macaddr> CODEC =
      new Codec<Macaddr>() {

        public String name() {
          return "macaddr";
        }

        @Override
        public int oid() {
          return 829;
        }

        @Override
        public int arrayOid() {
          return 1040;
        }

        @Override
        public void write(StringBuilder sb, Macaddr value) {
          sb.append(value);
        }

        @Override
        public Codec.ParsingResult<Macaddr> parse(CharSequence input, int offset)
            throws Codec.ParseException {
          // Format: xx:xx:xx:xx:xx:xx
          String s = input.subSequence(offset, input.length()).toString().trim();
          String[] parts = s.split(":");
          if (parts.length != 6) {
            throw new Codec.ParseException(input, offset, "Invalid macaddr: " + s);
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
            throw new Codec.ParseException(input, offset, "Invalid macaddr hex: " + s);
          }
        }

        @Override
        public byte[] encode(Macaddr value) {
          return new byte[] {
            value.b1(), value.b2(), value.b3(), value.b4(), value.b5(), value.b6()
          };
        }

        @Override
        public Macaddr decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
          if (length != 6) {
            throw new Codec.ParseException("Binary macaddr must be 6 bytes, got " + length);
          }
          return new Macaddr(buf.get(), buf.get(), buf.get(), buf.get(), buf.get(), buf.get());
        }
      };
}
