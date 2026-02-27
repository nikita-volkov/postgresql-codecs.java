package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

final class Int2Codec implements Codec<Short> {

    static final Int2Codec instance = new Int2Codec();

    private Int2Codec() {
    }

    public String name() {
        return "int2";
    }

    @Override
    public int oid() {
        return 21;
    }

    @Override
    public int arrayOid() {
        return 1005;
    }

    @Override
    public void bind(PreparedStatement ps, int index, Short value) throws SQLException {
        if (value != null) {
            ps.setShort(index, value);
        } else {
            ps.setNull(index, Types.SMALLINT);
        }
    }

    public void write(StringBuilder sb, Short value) {
        sb.append(value);
    }

    @Override
    public Codec.ParsingResult<Short> parse(CharSequence input, int offset) throws Codec.ParseException {
        int i = offset;
        int len = input.length();
        if (i >= len) {
            throw new Codec.ParseException(input, offset, "Expected int2, reached end of input");
        }
        boolean negative = input.charAt(i) == '-';
        if (negative || input.charAt(i) == '+') {
            i++;
        }
        if (i >= len || input.charAt(i) < '0' || input.charAt(i) > '9') {
            throw new Codec.ParseException(input, offset, "Expected int2 digits");
        }
        int value = 0;
        while (i < len && input.charAt(i) >= '0' && input.charAt(i) <= '9') {
            value = value * 10 + (input.charAt(i++) - '0');
        }
        return new Codec.ParsingResult<>((short) (negative ? -value : value), i);
    }

    @Override
    public byte[] encode(Short value) {
        return ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort(value).array();
    }

    @Override
    public Short decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 2) {
            throw new Codec.ParseException("Expected 2 bytes for int2, got " + length);
        }
        return buf.getShort();
    }

}
