package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

import io.pgenie.postgresqlcodecs.types.Macaddr8;

final class Macaddr8Codec implements Codec<Macaddr8> {

    static final Macaddr8Codec instance = new Macaddr8Codec();

    private Macaddr8Codec() {
    }

    public String name() {
        return "macaddr8";
    }

    @Override
    public int oid() {
        return 774;
    }

    @Override
    public int arrayOid() {
        return 775;
    }

    @Override
    public void bind(PreparedStatement ps, int index, Macaddr8 value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("macaddr8");
            obj.setValue(value.toString());
            ps.setObject(index, obj);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    @Override
    public void write(StringBuilder sb, Macaddr8 value) {
        sb.append(value);
    }

    @Override
    public Codec.ParsingResult<Macaddr8> parse(CharSequence input, int offset) throws Codec.ParseException {
        // Format: xx:xx:xx:xx:xx:xx:xx:xx
        String s = input.subSequence(offset, input.length()).toString().trim();
        String[] parts = s.split(":");
        if (parts.length != 8) {
            throw new Codec.ParseException(input, offset, "Invalid macaddr8: " + s);
        }
        try {
            byte b1 = (byte) Integer.parseInt(parts[0], 16);
            byte b2 = (byte) Integer.parseInt(parts[1], 16);
            byte b3 = (byte) Integer.parseInt(parts[2], 16);
            byte b4 = (byte) Integer.parseInt(parts[3], 16);
            byte b5 = (byte) Integer.parseInt(parts[4], 16);
            byte b6 = (byte) Integer.parseInt(parts[5], 16);
            byte b7 = (byte) Integer.parseInt(parts[6], 16);
            byte b8 = (byte) Integer.parseInt(parts[7], 16);
            return new Codec.ParsingResult<>(new Macaddr8(b1, b2, b3, b4, b5, b6, b7, b8), input.length());
        } catch (NumberFormatException e) {
            throw new Codec.ParseException(input, offset, "Invalid macaddr8 hex: " + s);
        }
    }

    @Override
    public byte[] encode(Macaddr8 value) {
        return new byte[]{
            value.b1(), value.b2(), value.b3(), value.b4(),
            value.b5(), value.b6(), value.b7(), value.b8()
        };
    }

    @Override
    public Macaddr8 decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 8) {
            throw new Codec.ParseException("Binary macaddr8 must be 8 bytes, got " + length);
        }
        return new Macaddr8(
                buf.get(), buf.get(), buf.get(), buf.get(),
                buf.get(), buf.get(), buf.get(), buf.get());
    }

}
