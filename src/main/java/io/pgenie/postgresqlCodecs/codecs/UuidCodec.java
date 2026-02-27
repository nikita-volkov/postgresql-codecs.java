package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

final class UuidCodec implements Codec<UUID> {

    static final UuidCodec instance = new UuidCodec();

    private UuidCodec() {
    }

    public String name() {
        return "uuid";
    }

    @Override
    public int oid() {
        return 2950;
    }

    @Override
    public int arrayOid() {
        return 2951;
    }

    @Override
    public void bind(PreparedStatement ps, int index, UUID value) throws SQLException {
        if (value != null) {
            ps.setObject(index, value);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    public void write(StringBuilder sb, UUID value) {
        sb.append(value.toString());
    }

    @Override
    public Codec.ParsingResult<UUID> parse(CharSequence input, int offset) throws Codec.ParseException {
        int end = offset + 36;
        if (end > input.length()) {
            throw new Codec.ParseException(input, offset, "Expected UUID (36 characters)");
        }
        String token = input.subSequence(offset, end).toString();
        try {
            java.util.UUID value = java.util.UUID.fromString(token);
            return new Codec.ParsingResult<>(value, end);
        } catch (IllegalArgumentException e) {
            throw new Codec.ParseException(input, offset, "Invalid UUID: " + token);
        }
    }

    @Override
    public byte[] encode(UUID value) {
        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
        buf.putLong(value.getMostSignificantBits());
        buf.putLong(value.getLeastSignificantBits());
        return buf.array();
    }

    @Override
    public UUID decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 16) {
            throw new Codec.ParseException("UuidCodec.decodeBinary: expected 16 bytes, got " + length);
        }
        long msb = buf.getLong();
        long lsb = buf.getLong();
        return new UUID(msb, lsb);
    }

}
