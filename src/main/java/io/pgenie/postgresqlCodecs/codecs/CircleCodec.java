package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.geometric.PGcircle;

final class CircleCodec implements Codec<PGcircle> {

    static final CircleCodec instance = new CircleCodec();

    private CircleCodec() {
    }

    public String name() {
        return "circle";
    }

    @Override
    public int oid() {
        return 718;
    }

    @Override
    public int arrayOid() {
        return 719;
    }

    @Override
    public void bind(PreparedStatement ps, int index, PGcircle value) throws SQLException {
        if (value != null) {
            ps.setObject(index, value);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    public void write(StringBuilder sb, PGcircle value) {
        sb.append(value.getValue());
    }

    @Override
    public Codec.ParsingResult<PGcircle> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected circle, reached end of input");
        }
        // Circle format: <(x,y),r>
        int end = offset;
        if (input.charAt(end) == '<') {
            end++;
            while (end < len && input.charAt(end) != '>') {
                end++;
            }
            if (end < len) end++; // consume '>'
        } else {
            while (end < len) end++;
        }
        String token = input.subSequence(offset, end).toString();
        try {
            PGcircle value = new PGcircle(token);
            return new Codec.ParsingResult<>(value, end);
        } catch (java.sql.SQLException e) {
            throw new Codec.ParseException(input, offset, "Invalid circle: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(PGcircle value) {
        java.nio.ByteBuffer buf = ByteBuffer.allocate(24).order(ByteOrder.BIG_ENDIAN);
        buf.putDouble(value.center.x);
        buf.putDouble(value.center.y);
        buf.putDouble(value.radius);
        return buf.array();
    }

    @Override
    public PGcircle decodeBinary(java.nio.ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 24) throw new Codec.ParseException("Binary circle must be 24 bytes, got " + length);
        double cx = buf.getDouble();
        double cy = buf.getDouble();
        double r = buf.getDouble();
        org.postgresql.geometric.PGpoint center = new org.postgresql.geometric.PGpoint(cx, cy);
        return new PGcircle(center, r);
    }

}
