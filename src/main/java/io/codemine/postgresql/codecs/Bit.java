package io.codemine.postgresql.codecs;

import java.util.Arrays;

/**
 * PostgreSQL {@code bit} / {@code varbit} type. A fixed- or variable-length bit string.
 *
 * @param length the number of bits
 * @param data the bit data as bytes (MSB first), with {@code ceil(length/8)} bytes; any padding
 *     bits in the last byte must be zero
 */
public record Bit(int length, byte[] data) {

  @Override
  public boolean equals(Object o) {
    return o instanceof Bit b && length == b.length && Arrays.equals(data, b.data);
  }

  @Override
  public int hashCode() {
    return 31 * length + Arrays.hashCode(data);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(length);
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    for (int i = 0; i < length; i++) {
      int byteIndex = i / 8;
      int bitIndex = 7 - (i % 8);
      sb.append((data[byteIndex] >> bitIndex) & 1);
    }
  }
}
