package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.geometric.PGbox;

final class BoxCodec implements Codec<PGbox> {

    static final BoxCodec instance = new BoxCodec();

    private BoxCodec() {
    }

    public String name() {
        return "box";
    }

    @Override
    public int oid() {
        return 603;
    }

    @Override
    public int arrayOid() {
        return 1020;
    }

    @Override
    public void bind(PreparedStatement ps, int index, PGbox value) throws SQLException {
        if (value != null) {
            ps.setObject(index, value);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    public void write(StringBuilder sb, PGbox value) {
        sb.append(value.getValue());
    }

    @Override
    public Codec.ParsingResult<PGbox> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected box, reached end of input");
        }
        // Box format: (x1,y1),(x2,y2) — consume all remaining
        String token = input.subSequence(offset, len).toString();
        try {
            PGbox value = new PGbox(token);
            return new Codec.ParsingResult<>(value, len);
        } catch (java.sql.SQLException e) {
            throw new Codec.ParseException(input, offset, "Invalid box: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(PGbox value) {
        java.nio.ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
        buf.putDouble(value.point[0].x);
        buf.putDouble(value.point[0].y);
        buf.putDouble(value.point[1].x);
        buf.putDouble(value.point[1].y);
        return buf.array();
    }

    @Override
    public PGbox decodeBinary(java.nio.ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 32) throw new Codec.ParseException("Binary box must be 32 bytes, got " + length);
        double highX = buf.getDouble();
        double highY = buf.getDouble();
        double lowX = buf.getDouble();
        double lowY = buf.getDouble();
        return new PGbox(highX, highY, lowX, lowY);
    }

}
