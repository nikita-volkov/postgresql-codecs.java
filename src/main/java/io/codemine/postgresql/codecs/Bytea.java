package io.codemine.postgresql.codecs;

import java.util.Arrays;
import java.util.HexFormat;

/** A wrapper around a byte array representing a PostgreSQL {@code bytea} value. */
public record Bytea(byte[] bytes) {

  private static final HexFormat HEX = HexFormat.of();

  @Override
  public boolean equals(Object o) {
    return o instanceof Bytea b && Arrays.equals(bytes, b.bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    appendInTextTo(sb);
    return sb.toString();
  }

  void appendInTextTo(StringBuilder sb) {
    sb.append("\\x");
    sb.append(HEX.formatHex(bytes));
  }
}
