package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.geometric.PGline;

final class LineCodec implements Codec<PGline> {

    static final LineCodec instance = new LineCodec();

    private LineCodec() {
    }

    public String name() {
        return "line";
    }

    @Override
    public int oid() {
        return 628;
    }

    @Override
    public int arrayOid() {
        return 629;
    }

    @Override
    public void bind(PreparedStatement ps, int index, PGline value) throws SQLException {
        if (value != null) {
            ps.setObject(index, value);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    public void write(StringBuilder sb, PGline value) {
        sb.append(value.getValue());
    }

    @Override
    public Codec.ParsingResult<PGline> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected line, reached end of input");
        }
        int end = offset;
        if (input.charAt(end) == '{') {
            end++;
            while (end < len && input.charAt(end) != '}') {
                end++;
            }
            if (end < len) end++; // consume '}'
        } else {
            // Consume until end or delimiter
            while (end < len) end++;
        }
        String token = input.subSequence(offset, end).toString();
        try {
            PGline value = new PGline(token);
            return new Codec.ParsingResult<>(value, end);
        } catch (java.sql.SQLException e) {
            throw new Codec.ParseException(input, offset, "Invalid line: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(PGline value) {
        java.nio.ByteBuffer buf = ByteBuffer.allocate(24).order(ByteOrder.BIG_ENDIAN);
        buf.putDouble(value.a);
        buf.putDouble(value.b);
        buf.putDouble(value.c);
        return buf.array();
    }

    @Override
    public PGline decodeBinary(java.nio.ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 24) throw new Codec.ParseException("Binary line must be 24 bytes, got " + length);
        double a = buf.getDouble();
        double b = buf.getDouble();
        double c = buf.getDouble();
        return new PGline(a, b, c);
    }

}
