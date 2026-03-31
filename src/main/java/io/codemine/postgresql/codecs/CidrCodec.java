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
  public void encodeInText(StringBuilder sb, Cidr value) {
    value.write(sb);
  }

  @Override
  public Codec.ParsingResult<Cidr> decodeInText(CharSequence input, int offset)
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
      if (netmask < 0 || netmask > 128) {
        throw new Exception("Invalid IPv6 CIDR netmask (expected 0-128): " + netmask);
      }
      int[] groups = InetCodec.parseIpV6Groups(addrPart);
      int[] words = new int[4];
      words[0] = (groups[0] << 16) | groups[1];
      words[1] = (groups[2] << 16) | groups[3];
      words[2] = (groups[4] << 16) | groups[5];
      words[3] = (groups[6] << 16) | groups[7];

      // Zero out host bits according to the netmask
      int maskBits = netmask;
      if (maskBits <= 0) {
        // /0: whole address space, all bits are host bits -> zero everything
        words[0] = 0;
        words[1] = 0;
        words[2] = 0;
        words[3] = 0;
      } else if (maskBits < 128) {
        for (int i = 0; i < 4; i++) {
          int wordStart = i * 32;
          if (maskBits >= wordStart + 32) {
            // Entire word is network bits, keep as-is
            continue;
          } else if (maskBits <= wordStart) {
            // Entire word is host bits, zero it
            words[i] = 0;
          } else {
            // Partial: zero out the host bits in this word
            int bitsToKeep = maskBits - wordStart;
            words[i] = (bitsToKeep == 0) ? 0 : (words[i] & (-1 << (32 - bitsToKeep)));
          }
        }
      }

      return new Cidr.V6(words[0], words[1], words[2], words[3], (byte) netmask);
    } else {
      // IPv4
      if (netmask < 0 || netmask > 32) {
        throw new Exception("Invalid IPv4 CIDR netmask (expected 0-32): " + netmask);
      }
      int addr = InetCodec.parseIpV4(addrPart);

      // Zero out host bits according to the netmask
      int maskBits = netmask;
      if (maskBits <= 0) {
        // /0: whole address space, all bits are host bits -> zero everything
        addr = 0;
      } else if (maskBits < 32) {
        addr &= (-1 << (32 - maskBits));
      }
      return new Cidr.V4(addr, (byte) netmask);
    }
  }
}
