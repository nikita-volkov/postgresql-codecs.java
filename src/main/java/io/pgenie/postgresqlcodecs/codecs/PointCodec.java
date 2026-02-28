package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.geometric.PGpoint;

final class PointCodec implements Codec<PGpoint> {

    static final PointCodec instance = new PointCodec();

    private PointCodec() {
    }

    public String name() {
        return "point";
    }

    @Override
    public int oid() {
        return 600;
    }

    @Override
    public int arrayOid() {
        return 1017;
    }

    @Override
    public void bind(PreparedStatement ps, int index, PGpoint value) throws SQLException {
        if (value != null) {
            ps.setObject(index, value);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    public void write(StringBuilder sb, PGpoint value) {
        sb.append('(').append(value.x).append(',').append(value.y).append(')');
    }

    @Override
    public Codec.ParsingResult<PGpoint> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected point, reached end of input");
        }
        // Find the closing parenthesis
        int end = offset;
        if (input.charAt(end) == '(') {
            int depth = 1;
            end++;
            while (end < len && depth > 0) {
                if (input.charAt(end) == '(') depth++;
                else if (input.charAt(end) == ')') depth--;
                end++;
            }
        } else {
            throw new Codec.ParseException(input, offset, "Expected '(' for point");
        }
        String token = input.subSequence(offset, end).toString();
        try {
            PGpoint value = new PGpoint(token);
            return new Codec.ParsingResult<>(value, end);
        } catch (java.sql.SQLException e) {
            throw new Codec.ParseException(input, offset, "Invalid point: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(PGpoint value) {
        java.nio.ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
        buf.putDouble(value.x);
        buf.putDouble(value.y);
        return buf.array();
    }

    @Override
    public PGpoint decodeBinary(java.nio.ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 16) throw new Codec.ParseException("Binary point must be 16 bytes, got " + length);
        double x = buf.getDouble();
        double y = buf.getDouble();
        return new PGpoint(x, y);
    }

}
