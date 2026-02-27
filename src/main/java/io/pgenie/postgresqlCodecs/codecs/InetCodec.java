package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

final class InetCodec implements Codec<String> {

    static final InetCodec instance = new InetCodec();

    private InetCodec() {
    }

    public String name() {
        return "inet";
    }

    @Override
    public int oid() {
        return 869;
    }

    @Override
    public int arrayOid() {
        return 1041;
    }

    @Override
    public void bind(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("inet");
            obj.setValue(value);
            ps.setObject(index, obj);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    public void write(StringBuilder sb, String value) {
        sb.append(value);
    }

    @Override
    public Codec.ParsingResult<String> parse(CharSequence input, int offset) throws Codec.ParseException {
        return new Codec.ParsingResult<>(input.subSequence(offset, input.length()).toString(), input.length());
    }

    @Override
    public byte[] encode(String value) {
        String addr;
        int maskBits;
        int slashPos = value.indexOf('/');
        if (slashPos >= 0) {
            addr = value.substring(0, slashPos);
            maskBits = Integer.parseInt(value.substring(slashPos + 1));
        } else {
            addr = value;
            maskBits = -1; // default: /32 for IPv4, /128 for IPv6
        }

        byte[] addrBytes;
        byte af;
        try {
            java.net.InetAddress inetAddr = java.net.InetAddress.getByName(addr);
            addrBytes = inetAddr.getAddress();
            af = (addrBytes.length == 4) ? (byte) 2 : (byte) 3;
            if (maskBits == -1) maskBits = addrBytes.length * 8;
        } catch (java.net.UnknownHostException e) {
            throw new RuntimeException("Invalid inet address: " + addr, e);
        }

        ByteBuffer buf = ByteBuffer.allocate(4 + addrBytes.length).order(ByteOrder.BIG_ENDIAN);
        buf.put(af);
        buf.put((byte) maskBits);
        buf.put((byte) 0); // is_cidr=0 for inet
        buf.put((byte) addrBytes.length);
        buf.put(addrBytes);
        return buf.array();
    }

    @Override
    public String decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 4) throw new Codec.ParseException("Binary inet too short: " + length);
        buf.get(); // af, ignored
        int maskBits = Byte.toUnsignedInt(buf.get());
        buf.get(); // is_cidr flag, ignored
        int nb = Byte.toUnsignedInt(buf.get());
        if (length != 4 + nb) throw new Codec.ParseException("Binary inet length mismatch");
        byte[] addrBytes = new byte[nb];
        buf.get(addrBytes);
        String hostStr;
        if (nb == 4) {
            // IPv4 — format as dotted-decimal
            hostStr = (addrBytes[0] & 0xFF) + "." + (addrBytes[1] & 0xFF) + "."
                    + (addrBytes[2] & 0xFF) + "." + (addrBytes[3] & 0xFF);
        } else {
            // IPv6 — use compressed notation (RFC 5952)
            hostStr = compressIpv6(addrBytes);
        }
        return hostStr + "/" + maskBits;
    }

    /**
     * Formats a 16-byte IPv6 address as compressed text (RFC 5952),
     * e.g. {@code ::1} instead of {@code 0:0:0:0:0:0:0:1}.
     *
     * <p>Java's {@link java.net.InetAddress#getHostAddress()} does not
     * guarantee compressed output on all JVMs, so we implement compression
     * ourselves.
     */
    static String compressIpv6(byte[] bytes) {
        int[] g = new int[8];
        for (int i = 0; i < 8; i++) {
            g[i] = ((bytes[i * 2] & 0xFF) << 8) | (bytes[i * 2 + 1] & 0xFF);
        }
        // Find the longest consecutive run of zero groups (min length 2)
        int elideStart = -1, elideLen = 0, curStart = -1, curLen = 0;
        for (int i = 0; i < 8; i++) {
            if (g[i] == 0) {
                if (curLen == 0) curStart = i;
                curLen++;
            } else {
                if (curLen >= 2 && curLen > elideLen) { elideLen = curLen; elideStart = curStart; }
                curLen = 0;
            }
        }
        if (curLen >= 2 && curLen > elideLen) { elideLen = curLen; elideStart = curStart; }

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

}
