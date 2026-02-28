package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

final class BitCodec implements Codec<Bit> {

    static final BitCodec instance = new BitCodec();

    private BitCodec() {
    }

    public String name() {
        return "bit";
    }

    @Override
    public int oid() {
        return 1560;
    }

    @Override
    public int arrayOid() {
        return 1561;
    }

    @Override
    public void bind(PreparedStatement ps, int index, Bit value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("bit");
            obj.setValue(value.toBitString());
            ps.setObject(index, obj);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    @Override
    public void write(StringBuilder sb, Bit value) {
        sb.append(value.toBitString());
    }

    @Override
    public Codec.ParsingResult<Bit> parse(CharSequence input, int offset) throws Codec.ParseException {
        String s = input.subSequence(offset, input.length()).toString().trim();
        return new Codec.ParsingResult<>(Bit.fromBitString(s), input.length());
    }

    @Override
    public byte[] encode(Bit value) {
        ByteBuffer buf = ByteBuffer.allocate(4 + value.bytes.length).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(value.numBits);
        buf.put(value.bytes);
        return buf.array();
    }

    @Override
    public Bit decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 4) throw new Codec.ParseException("Binary bit too short: " + length);
        int numBits = buf.getInt();
        int numBytes = (numBits + 7) / 8;
        if (length != 4 + numBytes) throw new Codec.ParseException("Binary bit length mismatch");
        byte[] data = new byte[numBytes];
        buf.get(data);
        return new Bit(numBits, data);
    }

}
