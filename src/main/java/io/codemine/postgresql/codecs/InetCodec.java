package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code inet} values. */
final class InetCodec implements Codec<Inet> {
  @Override
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
  public void encodeInText(StringBuilder sb, Inet value) {
    value.appendInTextTo(sb);
  }

  @Override
  public Codec.ParsingResult<Inet> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      return new Codec.ParsingResult<>(parseInet(s), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid inet: " + s);
    }
  }

  @Override
  public void encodeInBinary(Inet value, ByteArrayOutputStream out) {
    value.encodeInBinary(out);
  }

  @Override
  public Inet decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    if (length < 4) {
      throw new Codec.DecodingException("Binary inet too short: " + length);
    }
    byte af = buf.get();
    byte netmask = buf.get();
    buf.get(); // is_cidr flag, ignored
    int addrLen = Byte.toUnsignedInt(buf.get());
    return switch (af) {
      case 2 -> {
        if (addrLen != 4 || length != 8) {
          throw new Codec.DecodingException("Binary IPv4 inet length mismatch");
        }
        yield new Inet.V4(buf.getInt(), netmask);
      }
      case 3 -> {
        if (addrLen != 16 || length != 20) {
          throw new Codec.DecodingException("Binary IPv6 inet length mismatch");
        }
        yield new Inet.V6(buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt(), netmask);
      }
      default -> throw new Codec.DecodingException("Unknown inet address family: " + af);
    };
  }

  @Override
  public Inet random(Random r, int size) {
    if (r.nextBoolean()) {
      return new Inet.V4(r.nextInt(), (byte) r.nextInt(0, 33));
    } else {
      return new Inet.V6(
          r.nextInt(), r.nextInt(), r.nextInt(), r.nextInt(), (byte) r.nextInt(0, 129));
    }
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

  /** Parses a compressed or full IPv6 address string into an array of 8 unsigned 16-bit groups. */
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
}
