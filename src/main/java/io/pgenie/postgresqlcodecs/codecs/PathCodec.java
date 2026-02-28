package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.geometric.PGpath;

final class PathCodec implements Codec<PGpath> {

    static final PathCodec instance = new PathCodec();

    private PathCodec() {
    }

    public String name() {
        return "path";
    }

    @Override
    public int oid() {
        return 602;
    }

    @Override
    public int arrayOid() {
        return 1019;
    }

    @Override
    public void bind(PreparedStatement ps, int index, PGpath value) throws SQLException {
        if (value != null) {
            ps.setObject(index, value);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    public void write(StringBuilder sb, PGpath value) {
        sb.append(value.getValue());
    }

    @Override
    public Codec.ParsingResult<PGpath> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected path, reached end of input");
        }
        // Path is enclosed in [] (open) or () (closed) — consume to matching bracket
        int end = offset;
        char open = input.charAt(end);
        if (open == '[' || open == '(') {
            char close = (open == '[') ? ']' : ')';
            int depth = 1;
            end++;
            while (end < len && depth > 0) {
                char c = input.charAt(end);
                if (c == open) depth++;
                else if (c == close) depth--;
                end++;
            }
        } else {
            while (end < len) end++;
        }
        String token = input.subSequence(offset, end).toString();
        try {
            PGpath value = new PGpath(token);
            return new Codec.ParsingResult<>(value, end);
        } catch (java.sql.SQLException e) {
            throw new Codec.ParseException(input, offset, "Invalid path: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(PGpath value) {
        int n = value.points.length;
        java.nio.ByteBuffer buf = ByteBuffer.allocate(1 + 4 + n * 16).order(ByteOrder.BIG_ENDIAN);
        buf.put(value.open ? (byte) 0 : (byte) 1);
        buf.putInt(n);
        for (org.postgresql.geometric.PGpoint p : value.points) {
            buf.putDouble(p.x);
            buf.putDouble(p.y);
        }
        return buf.array();
    }

    @Override
    public PGpath decodeBinary(java.nio.ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 5) throw new Codec.ParseException("Binary path too short: " + length);
        byte closedByte = buf.get();
        boolean open = (closedByte == 0);
        int npts = buf.getInt();
        if (length != 1 + 4 + npts * 16) throw new Codec.ParseException("Binary path length mismatch");
        org.postgresql.geometric.PGpoint[] points = new org.postgresql.geometric.PGpoint[npts];
        for (int i = 0; i < npts; i++) {
            double x = buf.getDouble();
            double y = buf.getDouble();
            points[i] = new org.postgresql.geometric.PGpoint(x, y);
        }
        org.postgresql.geometric.PGpath path = new org.postgresql.geometric.PGpath(points, open);
        return path;
    }

}
