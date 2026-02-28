package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

final class CharCodec implements Codec<String> {

    static final CharCodec instance = new CharCodec();

    private CharCodec() {
    }

    public String name() {
        return "char";
    }

    @Override
    public int oid() {
        return 18;
    }

    @Override
    public int arrayOid() {
        return 1002;
    }

    @Override
    public void bind(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            ps.setString(index, value);
        } else {
            ps.setNull(index, Types.CHAR);
        }
    }

    public void write(StringBuilder sb, String value) {
        sb.append(value);
    }

    @Override
    public Codec.ParsingResult<String> parse(CharSequence input, int offset) throws Codec.ParseException {
        return new Codec.ParsingResult<>(input.subSequence(offset, input.length()).toString(), input.length());
    }

    @Override
    public byte[] encode(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        String result = new String(buf.array(), buf.arrayOffset() + buf.position(), length, StandardCharsets.UTF_8);
        buf.position(buf.position() + length);
        return result;
    }

}
