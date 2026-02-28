package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

final class TimestamptzCodec implements Codec<OffsetDateTime> {

    static final TimestamptzCodec instance = new TimestamptzCodec();

    private static final long PG_EPOCH_MICROS = 946684800_000_000L;

    private TimestamptzCodec() {
    }

    public String name() {
        return "timestamptz";
    }

    @Override
    public int oid() {
        return 1184;
    }

    @Override
    public int arrayOid() {
        return 1185;
    }

    @Override
    public void bind(PreparedStatement ps, int index, OffsetDateTime value) throws SQLException {
        if (value != null) {
            ps.setObject(index, value, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        }
    }

    public void write(StringBuilder sb, OffsetDateTime value) {
        sb.append(value.toString().replace('T', ' '));
    }

    @Override
    public Codec.ParsingResult<OffsetDateTime> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected timestamptz, reached end of input");
        }
        int p = offset;
        // Year: one or more digits (may be more than 4 for years after 9999 AD).
        int yearStart = p;
        while (p < len && Character.isDigit(input.charAt(p))) p++;
        if (p == yearStart || p >= len || input.charAt(p) != '-') {
            throw new Codec.ParseException(input, offset, "Expected timestamptz YYYY-MM-DD HH:mm:ss+tz");
        }
        int year = Integer.parseInt(input.subSequence(yearStart, p).toString());
        p++; // skip '-'
        // Month, '-', day, ' ', HH, ':', mm, ':', ss — need at least 14 more chars.
        if (p + 14 > len) {
            throw new Codec.ParseException(input, offset, "Expected timestamptz YYYY-MM-DD HH:mm:ss+tz");
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
        // Timezone offset: +HH, -HH, +HH:mm, -HH:mm, +HH:mm:ss, Z
        int tzSeconds = 0;
        if (p < len) {
            char c = input.charAt(p);
            if (c == '+' || c == '-') {
                int sign = (c == '+') ? 1 : -1;
                p++;
                int tzH = 0;
                while (p < len && input.charAt(p) >= '0' && input.charAt(p) <= '9') {
                    tzH = tzH * 10 + (input.charAt(p++) - '0');
                }
                int tzM = 0;
                if (p < len && input.charAt(p) == ':') {
                    p++;
                    while (p < len && input.charAt(p) >= '0' && input.charAt(p) <= '9') {
                        tzM = tzM * 10 + (input.charAt(p++) - '0');
                    }
                }
                int tzS = 0;
                if (p < len && input.charAt(p) == ':') {
                    p++;
                    while (p < len && input.charAt(p) >= '0' && input.charAt(p) <= '9') {
                        tzS = tzS * 10 + (input.charAt(p++) - '0');
                    }
                }
                tzSeconds = sign * (tzH * 3600 + tzM * 60 + tzS);
            } else if (c == 'Z') {
                p++;
            }
        }
        try {
            ZoneOffset tz = ZoneOffset.ofTotalSeconds(tzSeconds);
            OffsetDateTime value = OffsetDateTime.of(year, month, day, hour, minute, second, nanos, tz);
            return new Codec.ParsingResult<>(value, p);
        } catch (Exception e) {
            throw new Codec.ParseException(input, offset, "Invalid timestamptz: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(OffsetDateTime value) {
        OffsetDateTime utc = value.withOffsetSameInstant(ZoneOffset.UTC);
        long epochSecond = utc.toEpochSecond();
        int nanos = utc.getNano();
        long micros = epochSecond * 1_000_000L + nanos / 1000L - PG_EPOCH_MICROS;
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(micros).array();
    }

    @Override
    public OffsetDateTime decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 8) {
            throw new Codec.ParseException("TimestamptzCodec.decodeBinary: expected 8 bytes, got " + length);
        }
        long pgMicros = buf.getLong();
        long epochMicros = pgMicros + PG_EPOCH_MICROS;
        long epochSecond = epochMicros / 1_000_000L;
        int nanos = (int) (epochMicros % 1_000_000L) * 1000;
        if (nanos < 0) {
            epochSecond--;
            nanos += 1_000_000_000;
        }
        Instant instant = Instant.ofEpochSecond(epochSecond, nanos);
        return instant.atOffset(ZoneOffset.UTC);
    }

}
