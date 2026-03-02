package io.pgenie.postgresqlcodecs.types;

import io.pgenie.postgresqlcodecs.codecs.Codec;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * PostgreSQL {@code inet} type. IPv4 or IPv6 host address with optional subnet mask.
 *
 * <p>Holds an IPv4 or IPv6 host address, and optionally its subnet, all in one field. The subnet is
 * represented by the number of network address bits present in the host address (the "netmask"). If
 * the netmask is 32 and the address is IPv4 (or 128 for IPv6), the value represents just a single
 * host.
 *
 * <p>Port of the Haskell {@code PostgresqlTypes.Inet} sum type.
 */
public sealed interface Inet permits Inet.V4, Inet.V6 {

  /**
   * IPv4 host address with optional subnet mask.
   *
   * @param address IPv4 address as a 32-bit big-endian word.
   * @param netmask Network mask length in the range 0–32.
   */
  record V4(int address, byte netmask) implements Inet {}

  /**
   * IPv6 host address with optional subnet mask.
   *
   * @param w1 First 32 bits of the IPv6 address in big-endian order.
   * @param w2 Second 32 bits of the IPv6 address in big-endian order.
   * @param w3 Third 32 bits of the IPv6 address in big-endian order.
   * @param w4 Fourth 32 bits of the IPv6 address in big-endian order.
   * @param netmask Network mask length in the range 0–128.
   */
  record V6(int w1, int w2, int w3, int w4, byte netmask) implements Inet {}

  public static final Codec<Inet> CODEC =
      new Codec<Inet>() {

        public String name() {
          return "inet";
        }

        @Override
        public int scalarOid() {
          return 869;
        }

        @Override
        public int arrayOid() {
          return 1041;
        }

        @Override
        public void write(StringBuilder sb, Inet value) {
          sb.append(inetToText(value));
        }

        @Override
        public Codec.ParsingResult<Inet> parse(CharSequence input, int offset)
            throws Codec.ParseException {
          String s = input.subSequence(offset, input.length()).toString().trim();
          try {
            return new Codec.ParsingResult<>(parseInet(s), input.length());
          } catch (Exception e) {
            throw new Codec.ParseException(input, offset, "Invalid inet: " + s);
          }
        }

        @Override
        public void encodeInBinary(Inet value, ByteArrayOutputStream out) {
          switch (value) {
            case Inet.V4(int addr, byte netmask) -> {
              out.write(2); // IPv4 address family
              out.write(netmask);
              out.write(0); // is_cidr = 0 for inet
              out.write(4); // address length
              out.write((addr >>> 24) & 0xFF);
              out.write((addr >>> 16) & 0xFF);
              out.write((addr >>> 8) & 0xFF);
              out.write(addr & 0xFF);
            }
            case Inet.V6(int w1, int w2, int w3, int w4, byte netmask) -> {
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
            default -> throw new IllegalStateException("Unreachable: unknown Inet variant");
          }
        }

        @Override
        public Inet decodeInBinary(ByteBuffer buf, int length) throws Codec.ParseException {
          if (length < 4) {
            throw new Codec.ParseException("Binary inet too short: " + length);
          }
          byte af = buf.get();
          byte netmask = buf.get();
          buf.get(); // is_cidr flag, ignored
          int addrLen = Byte.toUnsignedInt(buf.get());
          return switch (af) {
            case 2 -> {
              if (addrLen != 4 || length != 8) {
                throw new Codec.ParseException("Binary IPv4 inet length mismatch");
              }
              yield new Inet.V4(buf.getInt(), netmask);
            }
            case 3 -> {
              if (addrLen != 16 || length != 20) {
                throw new Codec.ParseException("Binary IPv6 inet length mismatch");
              }
              yield new Inet.V6(buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt(), netmask);
            }
            default -> throw new Codec.ParseException("Unknown inet address family: " + af);
          };
        }

        // -----------------------------------------------------------------------
        // Text formatting helpers (package-visible for CidrCodec reuse)
        // -----------------------------------------------------------------------
        /** Formats an {@link Inet} value as its canonical PostgreSQL text representation. */
        static String inetToText(Inet value) {
          return switch (value) {
            case Inet.V4(int addr, byte netmask) -> {
              String ip = ipv4ToString(addr);
              yield (netmask & 0xff) == 32 ? ip : ip + "/" + (netmask & 0xff);
            }
            case Inet.V6(int w1, int w2, int w3, int w4, byte netmask) -> {
              String ip = ipv6ToString(w1, w2, w3, w4);
              yield (netmask & 0xff) == 128 ? ip : ip + "/" + (netmask & 0xff);
            }
          };
        }

        static String ipv4ToString(int addr) {
          return ((addr >>> 24) & 0xFF)
              + "."
              + ((addr >>> 16) & 0xFF)
              + "."
              + ((addr >>> 8) & 0xFF)
              + "."
              + (addr & 0xFF);
        }

        static int parseIpV4(String s) throws Exception {
          String[] parts = s.split("\\.");
          if (parts.length != 4) {
            throw new Exception("bad IPv4: " + s);
          }
          int addr = 0;
          for (String p : parts) {
            addr = (addr << 8) | Integer.parseInt(p);
          }
          return addr;
        }

        /** Parses an inet text value (with optional /n suffix) into an {@link Inet}. */
        static Inet parseInet(String s) throws Exception {
          int slash = s.indexOf('/');
          String addrPart = slash >= 0 ? s.substring(0, slash) : s;
          int netmask;

          if (addrPart.contains(":")) {
            // IPv6
            int[] groups = parseIpV6Groups(addrPart);
            int w1 = (groups[0] << 16) | groups[1];
            int w2 = (groups[2] << 16) | groups[3];
            int w3 = (groups[4] << 16) | groups[5];
            int w4 = (groups[6] << 16) | groups[7];
            netmask = slash >= 0 ? Integer.parseInt(s.substring(slash + 1)) : 128;
            return new Inet.V6(w1, w2, w3, w4, (byte) netmask);
          } else {
            // IPv4
            int addr = parseIpV4(addrPart);
            netmask = slash >= 0 ? Integer.parseInt(s.substring(slash + 1)) : 32;
            return new Inet.V4(addr, (byte) netmask);
          }
        }

        /**
         * Formats a 128-bit IPv6 address (stored as four 32-bit words) as compressed text per RFC
         * 5952, e.g. {@code ::1} instead of {@code 0:0:0:0:0:0:0:1}.
         */
        static String ipv6ToString(int w1, int w2, int w3, int w4) {
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

          StringBuilder sb = new StringBuilder(40);
          int i = 0;
          while (i < 8) {
            if (elideStart >= 0 && i == elideStart) {
              sb.append("::");
              i += elideLen;
            } else {
              if (sb.length() > 0 && sb.charAt(sb.length() - 1) != ':') {
                sb.append(':');
              }
              sb.append(Integer.toHexString(g[i]));
              i++;
            }
          }
          return sb.toString();
        }

        /**
         * Parses a compressed or full IPv6 address string into an array of 8 unsigned 16-bit
         * groups.
         */
        static int[] parseIpV6Groups(String s) throws Exception {
          // Handle :: expansion
          int dcolon = s.indexOf("::");
          if (dcolon >= 0) {
            String before = s.substring(0, dcolon);
            String after = s.substring(dcolon + 2);
            int[] beforeGroups = before.isEmpty() ? new int[0] : parseHexGroups(before);
            int[] afterGroups = after.isEmpty() ? new int[0] : parseHexGroups(after);
            int zeros = 8 - beforeGroups.length - afterGroups.length;
            if (zeros < 0) {
              throw new Exception("Too many groups in IPv6: " + s);
            }
            int[] result = new int[8];
            System.arraycopy(beforeGroups, 0, result, 0, beforeGroups.length);
            System.arraycopy(afterGroups, 0, result, 8 - afterGroups.length, afterGroups.length);
            return result;
          } else {
            int[] groups = parseHexGroups(s);
            if (groups.length != 8) {
              throw new Exception("Expected 8 groups in IPv6: " + s);
            }
            return groups;
          }
        }

        private static int[] parseHexGroups(String s) throws Exception {
          String[] parts = s.split(":");
          int[] result = new int[parts.length];
          for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i], 16);
          }
          return result;
        }

        /**
         * Generates a random {@code Inet} value — either an IPv4 or IPv6 address with a random host
         * address and a netmask that covers the full valid range.
         */
        @Override
        public Inet random(Random r) {
          if (r.nextBoolean()) {
            return new V4(r.nextInt(), (byte) r.nextInt(0, 33));
          } else {
            return new V6(
                r.nextInt(), r.nextInt(), r.nextInt(), r.nextInt(), (byte) r.nextInt(0, 129));
          }
        }
      };
}
