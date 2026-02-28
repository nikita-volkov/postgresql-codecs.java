package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.geometric.PGlseg;

final class LsegCodec implements Codec<PGlseg> {

    static final LsegCodec instance = new LsegCodec();

    private LsegCodec() {
    }

    public String name() {
        return "lseg";
    }

    @Override
    public int oid() {
        return 601;
    }

    @Override
    public int arrayOid() {
        return 1018;
    }

    @Override
    public void bind(PreparedStatement ps, int index, PGlseg value) throws SQLException {
        if (value != null) {
            ps.setObject(index, value);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    public void write(StringBuilder sb, PGlseg value) {
        sb.append(value.getValue());
    }

    @Override
    public Codec.ParsingResult<PGlseg> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected lseg, reached end of input");
        }
        int end = offset;
        if (input.charAt(end) == '[') {
            end++;
            while (end < len && input.charAt(end) != ']') {
                end++;
            }
            if (end < len) end++; // consume ']'
        } else {
            while (end < len) end++;
        }
        String token = input.subSequence(offset, end).toString();
        try {
            PGlseg value = new PGlseg(token);
            return new Codec.ParsingResult<>(value, end);
        } catch (java.sql.SQLException e) {
            throw new Codec.ParseException(input, offset, "Invalid lseg: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(PGlseg value) {
        java.nio.ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
        buf.putDouble(value.point[0].x);
        buf.putDouble(value.point[0].y);
        buf.putDouble(value.point[1].x);
        buf.putDouble(value.point[1].y);
        return buf.array();
    }

    @Override
    public PGlseg decodeBinary(java.nio.ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 32) throw new Codec.ParseException("Binary lseg must be 32 bytes, got " + length);
        double x1 = buf.getDouble();
        double y1 = buf.getDouble();
        double x2 = buf.getDouble();
        double y2 = buf.getDouble();
        return new PGlseg(x1, y1, x2, y2);
    }

}
