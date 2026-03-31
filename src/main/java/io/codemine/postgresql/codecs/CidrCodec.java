package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/** Codec for PostgreSQL {@code cidr} values. Uses the dedicated {@link Cidr} type. */
final class CidrCodec implements Codec<Cidr> {

  @Override
  public String name() {
    return "cidr";
  }

  @Override
  public int scalarOid() {
    return 650;
  }

  @Override
  public int arrayOid() {
    return 651;
  }

  @Override
  public void write(StringBuilder sb, Cidr value) {
    value.write(sb);
  }

  @Override
  public Codec.ParsingResult<Cidr> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      return new Codec.ParsingResult<>(parseCidr(s), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid cidr: " + s);
    }
  }

  @Override
  public void encodeInBinary(Cidr value, ByteArrayOutputStream out) {
    value.encodeInBinary(out);
  }

  @Override
  public Cidr decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    if (length < 4) {
      throw new Codec.DecodingException("Binary cidr too short: " + length);
    }
    byte af = buf.get();
    byte netmask = buf.get();
    buf.get(); // is_cidr flag, ignored
    int addrLen = Byte.toUnsignedInt(buf.get());
    return switch (af) {
      case 2 -> {
        if (addrLen != 4 || length != 8) {
          throw new Codec.DecodingException("Binary IPv4 cidr length mismatch");
        }
        yield new Cidr.V4(buf.getInt(), netmask);
      }
      case 3 -> {
        if (addrLen != 16 || length != 20) {
          throw new Codec.DecodingException("Binary IPv6 cidr length mismatch");
        }
        yield new Cidr.V6(buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt(), netmask);
      }
      default -> throw new Codec.DecodingException("Unknown cidr address family: " + af);
    };
  }

  @Override
  public Cidr random(Random r, int size) {
    if (r.nextBoolean()) {
      byte mask = (byte) r.nextInt(0, 33);
      int addr = r.nextInt();
      // Zero out host bits for valid CIDR
      int maskBits = mask & 0xff;
      if (maskBits == 0) {
        addr = 0;
      } else if (maskBits < 32) {
        addr &= (-1 << (32 - maskBits));
      }
      return new Cidr.V4(addr, mask);
    } else {
      byte mask = (byte) r.nextInt(0, 129);
      int[] words = {r.nextInt(), r.nextInt(), r.nextInt(), r.nextInt()};
      // Zero out host bits
      int maskBits = mask & 0xff;
      for (int i = 0; i < 4; i++) {
        int wordStart = i * 32;
        if (maskBits >= wordStart + 32) {
          // Entire word is network bits, keep as-is
        } else if (maskBits <= wordStart) {
          // Entire word is host bits, zero it
          words[i] = 0;
        } else {
          // Partial: zero out the host bits in this word
          int bitsToKeep = maskBits - wordStart;
          words[i] = (bitsToKeep == 0) ? 0 : (words[i] & (-1 << (32 - bitsToKeep)));
        }
      }
      return new Cidr.V6(words[0], words[1], words[2], words[3], mask);
    }
  }

  /** Parses a CIDR text value into a {@link Cidr}. */
  static Cidr parseCidr(String s) throws Exception {
    int slash = s.indexOf('/');
    if (slash < 0) {
      throw new Exception("cidr value must include a netmask: " + s);
    }
    String addrPart = s.substring(0, slash);
    int netmask = Integer.parseInt(s.substring(slash + 1));

    if (addrPart.contains(":")) {
      // IPv6
      int[] groups = InetCodec.parseIpV6Groups(addrPart);
      int w1 = (groups[0] << 16) | groups[1];
      int w2 = (groups[2] << 16) | groups[3];
      int w3 = (groups[4] << 16) | groups[5];
      int w4 = (groups[6] << 16) | groups[7];
      return new Cidr.V6(w1, w2, w3, w4, (byte) netmask);
    } else {
      // IPv4
      int addr = InetCodec.parseIpV4(addrPart);
      return new Cidr.V4(addr, (byte) netmask);
    }
  }
}
