package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

final class TimestampCodec implements Codec<LocalDateTime> {

    static final TimestampCodec instance = new TimestampCodec();

    private static final long PG_EPOCH_MICROS = 946684800_000_000L;

    private TimestampCodec() {
    }

    public String name() {
        return "timestamp";
    }

    @Override
    public int oid() {
        return 1114;
    }

    @Override
    public int arrayOid() {
        return 1115;
    }

    @Override
    public void bind(PreparedStatement ps, int index, LocalDateTime value) throws SQLException {
        if (value != null) {
            ps.setTimestamp(index, Timestamp.valueOf(value));
        } else {
            ps.setNull(index, Types.TIMESTAMP);
        }
    }

    public void write(StringBuilder sb, LocalDateTime value) {
        sb.append(value.toString().replace('T', ' '));
    }

    @Override
    public Codec.ParsingResult<LocalDateTime> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected timestamp, reached end of input");
        }
        int p = offset;
        // Year: one or more digits (may be more than 4 for years after 9999 AD).
        int yearStart = p;
        while (p < len && Character.isDigit(input.charAt(p))) p++;
        if (p == yearStart || p >= len || input.charAt(p) != '-') {
            throw new Codec.ParseException(input, offset, "Expected timestamp YYYY-MM-DD HH:mm:ss");
        }
        int year = Integer.parseInt(input.subSequence(yearStart, p).toString());
        p++; // skip '-'
        // Month (2 digits), '-', day (2 digits), ' ', HH, ':', mm, ':', ss
        // Need at least 14 more characters: MM-DD HH:mm:ss
        if (p + 14 > len) {
            throw new Codec.ParseException(input, offset, "Expected timestamp YYYY-MM-DD HH:mm:ss");
        }
        int month = (input.charAt(p) - '0') * 10 + (input.charAt(p + 1) - '0');
        p += 3; // skip MM-
        int day = (input.charAt(p) - '0') * 10 + (input.charAt(p + 1) - '0');
        p += 3; // skip DD[space]
        int hour = (input.charAt(p) - '0') * 10 + (input.charAt(p + 1) - '0');
        p += 3; // skip HH:
        int minute = (input.charAt(p) - '0') * 10 + (input.charAt(p + 1) - '0');
        p += 3; // skip mm:
        int second = (input.charAt(p) - '0') * 10 + (input.charAt(p + 1) - '0');
        p += 2;
        // Optional fractional seconds
        int nanos = 0;
        if (p < len && input.charAt(p) == '.') {
            p++;
            int fracStart = p;
            while (p < len && input.charAt(p) >= '0' && input.charAt(p) <= '9') p++;
            // Parse up to 9 digits (nanoseconds precision); pad or truncate to 9 digits
            int fracLen = p - fracStart;
            String fracStr = input.subSequence(fracStart, p).toString();
            if (fracLen <= 9) {
                nanos = Integer.parseInt(fracStr) * (int) Math.pow(10, 9 - fracLen);
            } else {
                nanos = Integer.parseInt(fracStr.substring(0, 9));
            }
        }
        // Optional " BC" suffix
        if (p + 3 <= len && input.charAt(p) == ' ' && input.charAt(p + 1) == 'B' && input.charAt(p + 2) == 'C') {
            year = -(year - 1);
            p += 3;
        }
        try {
            LocalDateTime value = LocalDateTime.of(year, month, day, hour, minute, second, nanos);
            return new Codec.ParsingResult<>(value, p);
        } catch (Exception e) {
            throw new Codec.ParseException(input, offset, "Invalid timestamp: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(LocalDateTime value) {
        long epochSecond = value.toEpochSecond(ZoneOffset.UTC);
        int nanos = value.getNano();
        long micros = epochSecond * 1_000_000L + nanos / 1000L - PG_EPOCH_MICROS;
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(micros).array();
    }

    @Override
    public LocalDateTime decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 8) {
            throw new Codec.ParseException("TimestampCodec.decodeBinary: expected 8 bytes, got " + length);
        }
        long pgMicros = buf.getLong();
        long epochMicros = pgMicros + PG_EPOCH_MICROS;
        long epochSecond = epochMicros / 1_000_000L;
        int nanos = (int) (epochMicros % 1_000_000L) * 1000;
        if (nanos < 0) {
            epochSecond--;
            nanos += 1_000_000_000;
        }
        return LocalDateTime.ofEpochSecond(epochSecond, nanos, ZoneOffset.UTC);
    }

}
