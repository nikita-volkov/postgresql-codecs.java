package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;

/**
 * PostgreSQL {@code inet} type. IPv4 or IPv6 host address with optional subnet mask.
 *
 * <p>Holds an IPv4 or IPv6 host address, and optionally its subnet, all in one field. The subnet is
 * represented by the number of network address bits present in the host address (the "netmask"). If
 * the netmask is 32 and the address is IPv4 (or 128 for IPv6), the value represents just a single
 * host.
 */
public sealed interface Inet permits Inet.V4, Inet.V6 {

  /** Appends the PostgreSQL text representation of this address to {@code sb}. */
  void appendInTextTo(StringBuilder sb);

  /** Encodes this address in PostgreSQL binary wire format into {@code out}. */
  void encodeInBinary(ByteArrayOutputStream out);

  /**
   * IPv4 host address with optional subnet mask.
   *
   * @param address IPv4 address as a 32-bit big-endian word.
   * @param netmask Network mask length in the range 0–32.
   */
  record V4(int address, byte netmask) implements Inet {
    @Override
    public void appendInTextTo(StringBuilder sb) {
      sb.append((address >>> 24) & 0xFF);
      sb.append('.');
      sb.append((address >>> 16) & 0xFF);
      sb.append('.');
      sb.append((address >>> 8) & 0xFF);
      sb.append('.');
      sb.append(address & 0xFF);
      if ((netmask & 0xff) != 32) {
        sb.append('/').append(netmask & 0xff);
      }
    }

    @Override
    public void encodeInBinary(ByteArrayOutputStream out) {
      out.write(2); // IPv4 address family
      out.write(netmask);
      out.write(0); // is_cidr = 0 for inet
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
   * IPv6 host address with optional subnet mask.
   *
   * @param w1 First 32 bits of the IPv6 address in big-endian order.
   * @param w2 Second 32 bits of the IPv6 address in big-endian order.
   * @param w3 Third 32 bits of the IPv6 address in big-endian order.
   * @param w4 Fourth 32 bits of the IPv6 address in big-endian order.
   * @param netmask Network mask length in the range 0–128.
   */
  record V6(int w1, int w2, int w3, int w4, byte netmask) implements Inet {
    @Override
    public void appendInTextTo(StringBuilder sb) {
      // Formats a 128-bit IPv6 address (stored as four 32-bit words) as compressed text per
      // RFC 5952, e.g. {@code ::1} instead of {@code 0:0:0:0:0:0:0:1}.

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

      if ((netmask & 0xff) != 128) {
        sb.append('/').append(netmask & 0xff);
      }
    }

    @Override
    public void encodeInBinary(ByteArrayOutputStream out) {
      out.write(3); // IPv6 address family for INET
      out.write(netmask);
      out.write(0); // is_cidr = 0 for inet
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
