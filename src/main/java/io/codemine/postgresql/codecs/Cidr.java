package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;

/**
 * PostgreSQL {@code cidr} type. Represents a network address — an IPv4 or IPv6 address whose host
 * bits are all zero.
 *
 * <p>Unlike {@link Inet}, which can hold an arbitrary host address with an optional subnet mask, a
 * {@code cidr} value always denotes a network: the address stored is the network address itself
 * (all host bits are zero), and the netmask is required.
 */
public sealed interface Cidr permits Cidr.V4, Cidr.V6 {

  /** Appends the PostgreSQL text representation of this CIDR block to {@code sb}. */
  void appendInTextTo(StringBuilder sb);

  /** Encodes this CIDR block in PostgreSQL binary wire format into {@code out}. */
  void encodeInBinary(ByteArrayOutputStream out);

  /**
   * IPv4 CIDR block.
   *
   * @param address IPv4 network address as a 32-bit big-endian word (host bits must be zero).
   * @param netmask Network mask length in the range 0–32.
   */
  record V4(int address, byte netmask) implements Cidr {

    /**
     * Canonical constructor that enforces the documented invariants:
     *
     * <ul>
     *   <li>{@code netmask} must be in the range 0–32 (inclusive)
     *   <li>all host bits of {@code address} beyond the netmask must be zero
     * </ul>
     *
     * @throws IllegalArgumentException if the arguments do not satisfy the invariants
     */
    public V4 {
      int nm = netmask & 0xFF; // treat as unsigned
      if (nm < 0 || nm > 32) {
        throw new IllegalArgumentException("Invalid IPv4 CIDR netmask length: " + nm);
      }
      if (nm > 0) {
        // Construct a mask with the top nm bits set (network bits).
        int maskBits = -1 << (32 - nm);
        if ((address & maskBits) != address) {
          throw new IllegalArgumentException(
              "IPv4 CIDR address has non-zero host bits for netmask /" + nm);
        }
      }
    }

    @Override
    public void appendInTextTo(StringBuilder sb) {
      sb.append((address >>> 24) & 0xFF);
      sb.append('.');
      sb.append((address >>> 16) & 0xFF);
      sb.append('.');
      sb.append((address >>> 8) & 0xFF);
      sb.append('.');
      sb.append(address & 0xFF);
      sb.append('/').append(netmask & 0xff);
    }

    @Override
    public void encodeInBinary(ByteArrayOutputStream out) {
      out.write(2); // IPv4 address family
      out.write(netmask);
      out.write(1); // is_cidr = 1
      out.write(4); // address length
      out.write((address >>> 24) & 0xFF);
      out.write((address >>> 16) & 0xFF);
      out.write((address >>> 8) & 0xFF);
      out.write(address & 0xFF);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      appendInTextTo(sb);
      return sb.toString();
    }
  }

  /**
   * IPv6 CIDR block.
   *
   * @param w1 First 32 bits of the IPv6 network address in big-endian order (host bits must be
   *     zero).
   * @param w2 Second 32 bits of the IPv6 network address in big-endian order.
   * @param w3 Third 32 bits of the IPv6 network address in big-endian order.
   * @param w4 Fourth 32 bits of the IPv6 network address in big-endian order.
   * @param netmask Network mask length in the range 0–128.
   */
  record V6(int w1, int w2, int w3, int w4, byte netmask) implements Cidr {

    /**
     * Canonical constructor that enforces the documented invariants:
     *
     * <ul>
     *   <li>{@code netmask} must be in the range 0–128 (inclusive)
     *   <li>all host bits of the address beyond the netmask must be zero
     * </ul>
     *
     * @throws IllegalArgumentException if the arguments do not satisfy the invariants
     */
    public V6 {
      int nm = netmask & 0xFF; // treat as unsigned
      if (nm > 128) {
        throw new IllegalArgumentException("Invalid IPv6 CIDR netmask length: " + nm);
      }
      int[] words = {w1, w2, w3, w4};
      for (int i = 0; i < 4; i++) {
        int wordStart = i * 32;
        if (nm >= wordStart + 32) {
          // Entire word is network bits — nothing to check.
        } else if (nm <= wordStart) {
          // Entire word is host bits — must be zero.
          if (words[i] != 0) {
            throw new IllegalArgumentException(
                "IPv6 CIDR address has non-zero host bits for netmask /" + nm);
          }
        } else {
          // Partial word — check host bits are zero.
          int bitsToKeep = nm - wordStart;
          int mask = -1 << (32 - bitsToKeep);
          if ((words[i] & mask) != words[i]) {
            throw new IllegalArgumentException(
                "IPv6 CIDR address has non-zero host bits for netmask /" + nm);
          }
        }
      }
    }

    @Override
    public void appendInTextTo(StringBuilder sb) {
      // Formats a 128-bit IPv6 network address (stored as four 32-bit words) as compressed text
      // per RFC 5952, always including the prefix length.

      int[] g = {
        (w1 >>> 16) & 0xFFFF, w1 & 0xFFFF,
        (w2 >>> 16) & 0xFFFF, w2 & 0xFFFF,
        (w3 >>> 16) & 0xFFFF, w3 & 0xFFFF,
        (w4 >>> 16) & 0xFFFF, w4 & 0xFFFF
      };
      // Find the longest consecutive run of zero groups (min length 2)
      int elideStart = -1;
      int elideLen = 0;
      int curStart = -1;
      int curLen = 0;
      for (int i = 0; i < 8; i++) {
        if (g[i] == 0) {
          if (curLen == 0) {
            curStart = i;
          }
          curLen++;
        } else {
          if (curLen >= 2 && curLen > elideLen) {
            elideLen = curLen;
            elideStart = curStart;
          }
          curLen = 0;
        }
      }
      if (curLen >= 2 && curLen > elideLen) {
        elideLen = curLen;
        elideStart = curStart;
      }

      boolean first = true;
      int i = 0;
      while (i < 8) {
        if (elideStart >= 0 && i == elideStart) {
          sb.append("::");
          i += elideLen;
          first = false;
        } else {
          if (!first) {
            sb.append(':');
          }
          sb.append(Integer.toHexString(g[i]));
          first = false;
          i++;
        }
      }

      sb.append('/').append(netmask & 0xff);
    }

    @Override
    public void encodeInBinary(ByteArrayOutputStream out) {
      out.write(3); // IPv6 address family
      out.write(netmask);
      out.write(1); // is_cidr = 1
      out.write(16); // address length
      out.write((w1 >>> 24) & 0xFF);
      out.write((w1 >>> 16) & 0xFF);
      out.write((w1 >>> 8) & 0xFF);
      out.write(w1 & 0xFF);
      out.write((w2 >>> 24) & 0xFF);
      out.write((w2 >>> 16) & 0xFF);
      out.write((w2 >>> 8) & 0xFF);
      out.write(w2 & 0xFF);
      out.write((w3 >>> 24) & 0xFF);
      out.write((w3 >>> 16) & 0xFF);
      out.write((w3 >>> 8) & 0xFF);
      out.write(w3 & 0xFF);
      out.write((w4 >>> 24) & 0xFF);
      out.write((w4 >>> 16) & 0xFF);
      out.write((w4 >>> 8) & 0xFF);
      out.write(w4 & 0xFF);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      appendInTextTo(sb);
      return sb.toString();
    }
  }
}
