package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.geometric.PGpolygon;

final class PolygonCodec implements Codec<PGpolygon> {

    static final PolygonCodec instance = new PolygonCodec();

    private PolygonCodec() {
    }

    public String name() {
        return "polygon";
    }

    @Override
    public int oid() {
        return 604;
    }

    @Override
    public int arrayOid() {
        return 1027;
    }

    @Override
    public void bind(PreparedStatement ps, int index, PGpolygon value) throws SQLException {
        if (value != null) {
            ps.setObject(index, value);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    public void write(StringBuilder sb, PGpolygon value) {
        sb.append(value.getValue());
    }

    @Override
    public Codec.ParsingResult<PGpolygon> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected polygon, reached end of input");
        }
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
            while (end < len) end++;
        }
        String token = input.subSequence(offset, end).toString();
        try {
            PGpolygon value = new PGpolygon(token);
            return new Codec.ParsingResult<>(value, end);
        } catch (java.sql.SQLException e) {
            throw new Codec.ParseException(input, offset, "Invalid polygon: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(PGpolygon value) {
        int n = value.points.length;
        java.nio.ByteBuffer buf = ByteBuffer.allocate(4 + n * 16).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(n);
        for (org.postgresql.geometric.PGpoint p : value.points) {
            buf.putDouble(p.x);
            buf.putDouble(p.y);
        }
        return buf.array();
    }

    @Override
    public PGpolygon decodeBinary(java.nio.ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 4) throw new Codec.ParseException("Binary polygon too short: " + length);
        int npts = buf.getInt();
        if (length != 4 + npts * 16) throw new Codec.ParseException("Binary polygon length mismatch");
        org.postgresql.geometric.PGpoint[] points = new org.postgresql.geometric.PGpoint[npts];
        for (int i = 0; i < npts; i++) {
            double x = buf.getDouble();
            double y = buf.getDouble();
            points[i] = new org.postgresql.geometric.PGpoint(x, y);
        }
        return new PGpolygon(points);
    }

}
