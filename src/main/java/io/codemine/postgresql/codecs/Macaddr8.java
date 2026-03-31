package io.codemine.postgresql.codecs;

/**
 * PostgreSQL {@code macaddr8} type. An 8-byte MAC address (EUI-64).
 *
 * <p>The canonical text format is {@code xx:xx:xx:xx:xx:xx:xx:xx} in lower-case hexadecimal.
 */
public record Macaddr8(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
  private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(23);
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    appendHexByte(sb, b1);
    sb.append(':');
    appendHexByte(sb, b2);
    sb.append(':');
    appendHexByte(sb, b3);
    sb.append(':');
    appendHexByte(sb, b4);
    sb.append(':');
    appendHexByte(sb, b5);
    sb.append(':');
    appendHexByte(sb, b6);
    sb.append(':');
    appendHexByte(sb, b7);
    sb.append(':');
    appendHexByte(sb, b8);
  }

  private static void appendHexByte(StringBuilder sb, byte value) {
    int unsigned = value & 0xff;
    sb.append(HEX_DIGITS[unsigned >>> 4]);
    sb.append(HEX_DIGITS[unsigned & 0x0f]);
  }
}
