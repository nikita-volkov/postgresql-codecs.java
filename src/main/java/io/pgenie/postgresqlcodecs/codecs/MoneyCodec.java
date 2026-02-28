package io.pgenie.postgresqlcodecs.codecs;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

final class MoneyCodec implements Codec<String> {

    static final MoneyCodec instance = new MoneyCodec();

    private MoneyCodec() {
    }

    public String name() {
        return "money";
    }

    @Override
    public int oid() {
        return 790;
    }

    @Override
    public int arrayOid() {
        return 791;
    }

    @Override
    public void bind(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("money");
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

}
