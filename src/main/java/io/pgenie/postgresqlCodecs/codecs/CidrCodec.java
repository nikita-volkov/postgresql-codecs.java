package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

final class CidrCodec implements Codec<String> {

    static final CidrCodec instance = new CidrCodec();

    private CidrCodec() {
    }

    public String name() {
        return "cidr";
    }

    @Override
    public int oid() {
        return 650;
    }

    @Override
    public int arrayOid() {
        return 651;
    }

    @Override
    public void bind(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("cidr");
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
            throw new RuntimeException("Invalid cidr address: " + addr, e);
        }

        ByteBuffer buf = ByteBuffer.allocate(4 + addrBytes.length).order(ByteOrder.BIG_ENDIAN);
        buf.put(af);
        buf.put((byte) maskBits);
        buf.put((byte) 1); // is_cidr=1 for cidr
        buf.put((byte) addrBytes.length);
        buf.put(addrBytes);
        return buf.array();
    }

    @Override
    public String decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 4) throw new Codec.ParseException("Binary cidr too short: " + length);
        buf.get(); // af, ignored
        int maskBits = Byte.toUnsignedInt(buf.get());
        buf.get(); // is_cidr flag, ignored
        int nb = Byte.toUnsignedInt(buf.get());
        if (length != 4 + nb) throw new Codec.ParseException("Binary cidr length mismatch");
        byte[] addrBytes = new byte[nb];
        buf.get(addrBytes);
        String hostStr;
        if (nb == 4) {
            hostStr = (addrBytes[0] & 0xFF) + "." + (addrBytes[1] & 0xFF) + "."
                    + (addrBytes[2] & 0xFF) + "." + (addrBytes[3] & 0xFF);
        } else {
            hostStr = InetCodec.compressIpv6(addrBytes);
        }
        return hostStr + "/" + maskBits;
    }

}
