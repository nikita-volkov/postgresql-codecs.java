package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

final class JsonbCodec implements Codec<String> {

    static final JsonbCodec instance = new JsonbCodec();

    private JsonbCodec() {
    }

    public String name() {
        return "jsonb";
    }

    @Override
    public int oid() {
        return 3802;
    }

    @Override
    public int arrayOid() {
        return 3807;
    }

    @Override
    public void bind(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("jsonb");
            obj.setValue(value);
            ps.setObject(index, obj);
        } else {
            ps.setNull(index, Types.OTHER);
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
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[1 + utf8.length];
        result[0] = 1;
        System.arraycopy(utf8, 0, result, 1, utf8.length);
        return result;
    }

    @Override
    public String decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 1) {
            throw new Codec.ParseException("Expected at least 1 byte for jsonb, got " + length);
        }
        buf.get(); // consume version byte
        String result = new String(buf.array(), buf.arrayOffset() + buf.position(), length - 1, StandardCharsets.UTF_8);
        buf.position(buf.position() + (length - 1));
        return result;
    }

}
