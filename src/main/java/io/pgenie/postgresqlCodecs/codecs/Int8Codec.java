package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

final class Int8Codec implements Codec<Long> {

    public static final Int8Codec instance = new Int8Codec();

    public String name() {
        return "int8";
    }

    @Override
    public int oid() {
        return 20;
    }

    @Override
    public int arrayOid() {
        return 1016;
    }

    @Override
    public void bind(PreparedStatement ps, int index, Long value) throws SQLException {
        if (value != null) {
            ps.setLong(index, value);
        } else {
            ps.setNull(index, Types.BIGINT);
        }
    }

    public void write(StringBuilder sb, Long value) {
        sb.append(value);
    }

    @Override
    public Codec.ParsingResult<Long> parse(CharSequence input, int offset) throws Codec.ParseException {
        int i = offset;
        int len = input.length();
        if (i >= len) {
            throw new Codec.ParseException(input, offset, "Expected int8, reached end of input");
        }
        boolean negative = input.charAt(i) == '-';
        if (negative || input.charAt(i) == '+') {
            i++;
        }
        if (i >= len || input.charAt(i) < '0' || input.charAt(i) > '9') {
            throw new Codec.ParseException(input, offset, "Expected int8 digits");
        }
        long value = 0;
        while (i < len && input.charAt(i) >= '0' && input.charAt(i) <= '9') {
            value = value * 10 + (input.charAt(i++) - '0');
        }
        return new Codec.ParsingResult<>(negative ? -value : value, i);
    }

    @Override
    public byte[] encode(Long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    }

    @Override
    public Long decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 8) {
            throw new Codec.ParseException("Expected 8 bytes for int8, got " + length);
        }
        return buf.getLong();
    }

}
