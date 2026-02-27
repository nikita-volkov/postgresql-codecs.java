package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import org.postgresql.util.PGobject;

final class TimetzCodec implements Codec<OffsetTime> {

    static final TimetzCodec instance = new TimetzCodec();

    private TimetzCodec() {
    }

    public String name() {
        return "timetz";
    }

    @Override
    public int oid() {
        return 1266;
    }

    @Override
    public int arrayOid() {
        return 1270;
    }

    @Override
    public void bind(PreparedStatement ps, int index, OffsetTime value) throws SQLException {
        PGobject obj = new PGobject();
        obj.setType("timetz");
        if (value != null) {
            obj.setValue(value.toString());
        }
        ps.setObject(index, obj);
    }

    public void write(StringBuilder sb, OffsetTime value) {
        sb.append(value.toString());
    }

    @Override
    public Codec.ParsingResult<OffsetTime> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected timetz, reached end of input");
        }
        // Minimum: "HH:mm:ss+HH" = 11 chars
        int i = offset + 8;
        if (i > len) {
            throw new Codec.ParseException(input, offset, "Expected timetz (HH:mm:ss±HH[:mm])");
        }
        // Optional fractional seconds
        if (i < len && input.charAt(i) == '.') {
            i++;
            while (i < len && input.charAt(i) >= '0' && input.charAt(i) <= '9') {
                i++;
            }
        }
        // Timezone offset
        if (i < len) {
            char c = input.charAt(i);
            if (c == '+' || c == '-') {
                i++;
                while (i < len && input.charAt(i) >= '0' && input.charAt(i) <= '9') {
                    i++;
                }
                if (i < len && input.charAt(i) == ':') {
                    i++;
                    while (i < len && input.charAt(i) >= '0' && input.charAt(i) <= '9') {
                        i++;
                    }
                    if (i < len && input.charAt(i) == ':') {
                        i++;
                        while (i < len && input.charAt(i) >= '0' && input.charAt(i) <= '9') {
                            i++;
                        }
                    }
                }
            } else if (c == 'Z') {
                i++;
            }
        }
        String token = input.subSequence(offset, i).toString();
        try {
            OffsetTime value = OffsetTime.parse(token, PARSER);
            return new Codec.ParsingResult<>(value, i);
        } catch (java.time.format.DateTimeParseException e) {
            throw new Codec.ParseException(input, offset, "Invalid timetz: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(OffsetTime value) {
        long micros = value.toLocalTime().toNanoOfDay() / 1000L;
        int pgTz = -value.getOffset().getTotalSeconds();
        ByteBuffer buf = ByteBuffer.allocate(12).order(ByteOrder.BIG_ENDIAN);
        buf.putLong(micros);
        buf.putInt(pgTz);
        return buf.array();
    }

    @Override
    public OffsetTime decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 12) {
            throw new Codec.ParseException("TimetzCodec.decodeBinary: expected 12 bytes, got " + length);
        }
        long micros = buf.getLong();
        int pgTz = buf.getInt();
        java.time.LocalTime lt = java.time.LocalTime.ofNanoOfDay(micros * 1000L);
        java.time.ZoneOffset zo = java.time.ZoneOffset.ofTotalSeconds(-pgTz);
        return lt.atOffset(zo);
    }

    private static final DateTimeFormatter PARSER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .optionalStart().appendOffset("+HH:MM:ss", "Z").optionalEnd()
            .optionalStart().appendOffset("+HH:MM", "Z").optionalEnd()
            .optionalStart().appendOffset("+HH", "Z").optionalEnd()
            .toFormatter();

}
