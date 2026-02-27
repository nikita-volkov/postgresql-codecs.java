package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

final class VarbitCodec implements Codec<String> {

    static final VarbitCodec instance = new VarbitCodec();

    private VarbitCodec() {
    }

    public String name() {
        return "varbit";
    }

    @Override
    public void bind(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("varbit");
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
        int nbits = value.length();
        int nbytes = (nbits + 7) / 8;
        java.nio.ByteBuffer buf = ByteBuffer.allocate(4 + nbytes).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(nbits);
        for (int i = 0; i < nbytes; i++) {
            int b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int pos = i * 8 + bit;
                if (pos < nbits && value.charAt(pos) == '1') {
                    b |= (0x80 >>> bit);
                }
            }
            buf.put((byte) b);
        }
        return buf.array();
    }

    @Override
    public String decodeBinary(java.nio.ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 4) throw new Codec.ParseException("Binary bit too short: " + length);
        int nbits = buf.getInt();
        int nbytes = (nbits + 7) / 8;
        if (length != 4 + nbytes) throw new Codec.ParseException("Binary bit length mismatch");
        StringBuilder sb = new StringBuilder(nbits);
        for (int i = 0; i < nbytes; i++) {
            int b = Byte.toUnsignedInt(buf.get());
            for (int bit = 0; bit < 8; bit++) {
                if (sb.length() < nbits) {
                    sb.append((b & (0x80 >>> bit)) != 0 ? '1' : '0');
                }
            }
        }
        return sb.toString();
    }

}
