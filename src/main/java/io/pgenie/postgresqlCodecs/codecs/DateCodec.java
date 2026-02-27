package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;

final class DateCodec implements Codec<LocalDate> {

    static final DateCodec instance = new DateCodec();

    private DateCodec() {
    }

    public String name() {
        return "date";
    }

    @Override
    public int oid() {
        return 1082;
    }

    @Override
    public int arrayOid() {
        return 1182;
    }

    @Override
    public void bind(PreparedStatement ps, int index, LocalDate value) throws SQLException {
        if (value != null) {
            ps.setDate(index, Date.valueOf(value));
        } else {
            ps.setNull(index, Types.DATE);
        }
    }

    public void write(StringBuilder sb, LocalDate value) {
        sb.append(value);
    }

    @Override
    public Codec.ParsingResult<LocalDate> parse(CharSequence input, int offset) throws Codec.ParseException {
        // ISO local date format: YYYY-MM-DD (10 characters)
        int end = offset + 10;
        if (end > input.length()) {
            throw new Codec.ParseException(input, offset, "Expected ISO date (YYYY-MM-DD)");
        }
        try {
            LocalDate value = LocalDate.parse(input.subSequence(offset, end));
            return new Codec.ParsingResult<>(value, end);
        } catch (java.time.format.DateTimeParseException e) {
            throw new Codec.ParseException(input, offset, e.getMessage());
        }
    }

    @Override
    public byte[] encode(LocalDate value) {
        long pgDay = value.toEpochDay() - 10957L;
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt((int) pgDay).array();
    }

    @Override
    public LocalDate decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 4) {
            throw new Codec.ParseException("DateCodec.decodeBinary: expected 4 bytes, got " + length);
        }
        int pgDay = buf.getInt();
        return LocalDate.ofEpochDay(pgDay + 10957L);
    }

}
