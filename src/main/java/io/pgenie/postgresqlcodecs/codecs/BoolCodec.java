package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

final class BoolCodec implements Codec<Boolean> {

    static final BoolCodec instance = new BoolCodec();

    private BoolCodec() {
    }

    public String name() {
        return "bool";
    }

    @Override
    public int oid() {
        return 16;
    }

    @Override
    public int arrayOid() {
        return 1000;
    }

    @Override
    public void bind(PreparedStatement ps, int index, Boolean value) throws SQLException {
        if (value != null) {
            ps.setBoolean(index, value);
        } else {
            ps.setNull(index, Types.BOOLEAN);
        }
    }

    public void write(StringBuilder sb, Boolean value) {
        sb.append(value ? "t" : "f");
    }

    @Override
    public Codec.ParsingResult<Boolean> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected bool, reached end of input");
        }
        int remaining = len - offset;
        String sub = input.subSequence(offset, len).toString().toLowerCase();
        for (String t : new String[]{"true", "on", "yes"}) {
            if (sub.startsWith(t)) {
                return new Codec.ParsingResult<>(true, offset + t.length());
            }
        }
        for (String f : new String[]{"false", "off"}) {
            if (sub.startsWith(f)) {
                return new Codec.ParsingResult<>(false, offset + f.length());
            }
        }
        if (sub.startsWith("no")) {
            return new Codec.ParsingResult<>(false, offset + 2);
        }
        char c = sub.charAt(0);
        if (c == 't' || c == '1' || c == 'y') {
            return new Codec.ParsingResult<>(true, offset + 1);
        }
        if (c == 'f' || c == '0' || c == 'n') {
            return new Codec.ParsingResult<>(false, offset + 1);
        }
        throw new Codec.ParseException(input, offset, "Expected bool value");
    }

    @Override
    public byte[] encode(Boolean value) {
        return new byte[]{value ? (byte) 1 : (byte) 0};
    }

    @Override
    public Boolean decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 1) {
            throw new Codec.ParseException("Expected 1 byte for bool, got " + length);
        }
        return buf.get() != 0;
    }

}
