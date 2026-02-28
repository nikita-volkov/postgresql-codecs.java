package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

final class CidrCodec implements Codec<Cidr> {

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
    public void bind(PreparedStatement ps, int index, Cidr value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("cidr");
            obj.setValue(InetCodec.cidrToText(value));
            ps.setObject(index, obj);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    @Override
    public void write(StringBuilder sb, Cidr value) {
        sb.append(InetCodec.cidrToText(value));
    }

    @Override
    public Codec.ParsingResult<Cidr> parse(CharSequence input, int offset) throws Codec.ParseException {
        String s = input.subSequence(offset, input.length()).toString().trim();
        try {
            return new Codec.ParsingResult<>(InetCodec.parseCidr(s), input.length());
        } catch (Exception e) {
            throw new Codec.ParseException(input, offset, "Invalid cidr: " + s);
        }
    }

    @Override
    public byte[] encode(Cidr value) {
        return switch (value) {
            case Cidr.V4(int addr, byte netmask) -> {
                ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                buf.put((byte) 2);        // IPv4 address family
                buf.put(netmask);
                buf.put((byte) 1);        // is_cidr = 1 for cidr
                buf.put((byte) 4);        // address length
                buf.putInt(addr);
                yield buf.array();
            }
            case Cidr.V6(int w1, int w2, int w3, int w4, byte netmask) -> {
                ByteBuffer buf = ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN);
                buf.put((byte) 3);        // IPv6 address family
                buf.put(netmask);
                buf.put((byte) 1);        // is_cidr = 1 for cidr
                buf.put((byte) 16);       // address length
                buf.putInt(w1);
                buf.putInt(w2);
                buf.putInt(w3);
                buf.putInt(w4);
                yield buf.array();
            }
        };
    }

    @Override
    public Cidr decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 4) throw new Codec.ParseException("Binary cidr too short: " + length);
        byte af = buf.get();
        byte netmask = buf.get();
        buf.get(); // is_cidr flag, ignored
        int addrLen = Byte.toUnsignedInt(buf.get());
        return switch (af) {
            case 2 -> {
                if (addrLen != 4 || length != 8) throw new Codec.ParseException("Binary IPv4 cidr length mismatch");
                yield new Cidr.V4(buf.getInt(), netmask);
            }
            case 3 -> {
                if (addrLen != 16 || length != 20) throw new Codec.ParseException("Binary IPv6 cidr length mismatch");
                yield new Cidr.V6(buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt(), netmask);
            }
            default -> throw new Codec.ParseException("Unknown cidr address family: " + af);
        };
    }

}
