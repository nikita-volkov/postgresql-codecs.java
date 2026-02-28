package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

import io.pgenie.postgresqlcodecs.types.Varbit;

final class VarbitCodec implements Codec<Varbit> {

    static final VarbitCodec instance = new VarbitCodec();

    private VarbitCodec() {
    }

    public String name() {
        return "varbit";
    }

    @Override
    public int oid() {
        return 1562;
    }

    @Override
    public int arrayOid() {
        return 1563;
    }

    @Override
    public void bind(PreparedStatement ps, int index, Varbit value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("varbit");
            obj.setValue(value.toBitString());
            ps.setObject(index, obj);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    @Override
    public void write(StringBuilder sb, Varbit value) {
        sb.append(value.toBitString());
    }

    @Override
    public Codec.ParsingResult<Varbit> parse(CharSequence input, int offset) throws Codec.ParseException {
        String s = input.subSequence(offset, input.length()).toString().trim();
        return new Codec.ParsingResult<>(Varbit.fromBitString(s), input.length());
    }

    @Override
    public byte[] encode(Varbit value) {
        ByteBuffer buf = ByteBuffer.allocate(4 + value.bytes.length).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(value.numBits);
        buf.put(value.bytes);
        return buf.array();
    }

    @Override
    public Varbit decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 4) throw new Codec.ParseException("Binary varbit too short: " + length);
        int numBits = buf.getInt();
        int numBytes = (numBits + 7) / 8;
        if (length != 4 + numBytes) throw new Codec.ParseException("Binary varbit length mismatch");
        byte[] data = new byte[numBytes];
        buf.get(data);
        return new Varbit(numBits, data);
    }

}
