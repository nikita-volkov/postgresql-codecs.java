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
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected date, reached end of input");
        }
        int p = offset;
        // Year: one or more digits (may be more than 4 for dates after 9999 AD).
        int yearStart = p;
        while (p < len && Character.isDigit(input.charAt(p))) p++;
        if (p == yearStart || p >= len || input.charAt(p) != '-') {
            throw new Codec.ParseException(input, offset, "Expected date YYYY-MM-DD");
        }
        int year = Integer.parseInt(input.subSequence(yearStart, p).toString());
        p++; // skip '-'
        // Month: 2 digits
        if (p + 1 >= len) {
            throw new Codec.ParseException(input, offset, "Expected month in date");
        }
        int month = (input.charAt(p) - '0') * 10 + (input.charAt(p + 1) - '0');
        p += 2;
        if (p >= len || input.charAt(p) != '-') {
            throw new Codec.ParseException(input, offset, "Expected '-' after month");
        }
        p++; // skip '-'
        // Day: 2 digits
        if (p + 1 >= len) {
            throw new Codec.ParseException(input, offset, "Expected day in date");
        }
        int day = (input.charAt(p) - '0') * 10 + (input.charAt(p + 1) - '0');
        p += 2;
        // Optional " BC" suffix: PostgreSQL uses "YYYY BC" for years before 1 AD.
        // In ISO proleptic calendar: 1 BC = year 0, 2 BC = year -1, etc.
        if (p + 3 <= len && input.charAt(p) == ' ' && input.charAt(p + 1) == 'B' && input.charAt(p + 2) == 'C') {
            year = -(year - 1);
            p += 3;
        }
        try {
            return new Codec.ParsingResult<>(LocalDate.of(year, month, day), p);
        } catch (Exception e) {
            throw new Codec.ParseException(input, offset, "Invalid date: " + e.getMessage());
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
