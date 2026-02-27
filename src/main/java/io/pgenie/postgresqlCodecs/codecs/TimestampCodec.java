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
        // Minimum: "yyyy-MM-dd HH:mm:ss" = 19 chars
        int i = offset + 19;
        if (i > len) {
            throw new Codec.ParseException(input, offset, "Expected timestamp (yyyy-MM-dd HH:mm:ss)");
        }
        // Optional fractional seconds
        if (i < len && input.charAt(i) == '.') {
            i++;
            while (i < len && input.charAt(i) >= '0' && input.charAt(i) <= '9') {
                i++;
            }
        }
        String token = input.subSequence(offset, i).toString().replace(' ', 'T');
        try {
            LocalDateTime value = LocalDateTime.parse(token);
            return new Codec.ParsingResult<>(value, i);
        } catch (java.time.format.DateTimeParseException e) {
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
