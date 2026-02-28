package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

final class MacaddrCodec implements Codec<Macaddr> {

    static final MacaddrCodec instance = new MacaddrCodec();

    private MacaddrCodec() {
    }

    public String name() {
        return "macaddr";
    }

    @Override
    public int oid() {
        return 829;
    }

    @Override
    public int arrayOid() {
        return 1040;
    }

    @Override
    public void bind(PreparedStatement ps, int index, Macaddr value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("macaddr");
            obj.setValue(value.toString());
            ps.setObject(index, obj);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    @Override
    public void write(StringBuilder sb, Macaddr value) {
        sb.append(value);
    }

    @Override
    public Codec.ParsingResult<Macaddr> parse(CharSequence input, int offset) throws Codec.ParseException {
        // Format: xx:xx:xx:xx:xx:xx
        String s = input.subSequence(offset, input.length()).toString().trim();
        String[] parts = s.split(":");
        if (parts.length != 6) {
            throw new Codec.ParseException(input, offset, "Invalid macaddr: " + s);
        }
        try {
            byte b1 = (byte) Integer.parseInt(parts[0], 16);
            byte b2 = (byte) Integer.parseInt(parts[1], 16);
            byte b3 = (byte) Integer.parseInt(parts[2], 16);
            byte b4 = (byte) Integer.parseInt(parts[3], 16);
            byte b5 = (byte) Integer.parseInt(parts[4], 16);
            byte b6 = (byte) Integer.parseInt(parts[5], 16);
            return new Codec.ParsingResult<>(new Macaddr(b1, b2, b3, b4, b5, b6), input.length());
        } catch (NumberFormatException e) {
            throw new Codec.ParseException(input, offset, "Invalid macaddr hex: " + s);
        }
    }

    @Override
    public byte[] encode(Macaddr value) {
        return new byte[]{value.b1(), value.b2(), value.b3(), value.b4(), value.b5(), value.b6()};
    }

    @Override
    public Macaddr decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 6) {
            throw new Codec.ParseException("Binary macaddr must be 6 bytes, got " + length);
        }
        return new Macaddr(buf.get(), buf.get(), buf.get(), buf.get(), buf.get(), buf.get());
    }

}
