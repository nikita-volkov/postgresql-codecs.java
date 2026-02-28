package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

final class VarcharCodec implements Codec<String> {

    static final VarcharCodec instance = new VarcharCodec();

    private VarcharCodec() {
    }

    public String name() {
        return "varchar";
    }

    @Override
    public int oid() {
        return 1043;
    }

    @Override
    public int arrayOid() {
        return 1015;
    }

    @Override
    public void bind(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            ps.setString(index, value);
        } else {
            ps.setNull(index, Types.VARCHAR);
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
