package io.codemine.postgresql.codecs;

/**
 * PostgreSQL {@code macaddr} type. MAC (Media Access Control) address.
 *
 * <p>Represents a 6-byte MAC address stored as six individual bytes. The canonical text format is
 * {@code xx:xx:xx:xx:xx:xx} in lower-case hexadecimal.
 */
public record Macaddr(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6) {
  private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(17);
    write(builder);
    return builder.toString();
  }

  /** Writes the MAC address to the given {@link StringBuilder} in the SQL format. */
  public void write(StringBuilder builder) {
    appendHexByte(builder, b1);
    builder.append(':');
    appendHexByte(builder, b2);
    builder.append(':');
    appendHexByte(builder, b3);
    builder.append(':');
    appendHexByte(builder, b4);
    builder.append(':');
    appendHexByte(builder, b5);
    builder.append(':');
    appendHexByte(builder, b6);
  }

  private static void appendHexByte(StringBuilder builder, byte value) {
    int unsignedValue = value & 0xff;
    builder.append(HEX_DIGITS[unsignedValue >>> 4]);
    builder.append(HEX_DIGITS[unsignedValue & 0x0f]);
  }
}
