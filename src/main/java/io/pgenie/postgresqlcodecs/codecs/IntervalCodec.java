package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.util.PGobject;

import io.pgenie.postgresqlcodecs.types.Interval;

final class IntervalCodec implements Codec<Interval> {

    static final IntervalCodec instance = new IntervalCodec();

    private IntervalCodec() {
    }

    public String name() {
        return "interval";
    }

    @Override
    public int oid() {
        return 1186;
    }

    @Override
    public int arrayOid() {
        return 1187;
    }

    @Override
    public void bind(PreparedStatement ps, int index, Interval value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("interval");
            obj.setValue(intervalToIso8601(value));
            ps.setObject(index, obj);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    @Override
    public void write(StringBuilder sb, Interval value) {
        sb.append(intervalToIso8601(value));
    }

    @Override
    public Codec.ParsingResult<Interval> parse(CharSequence input, int offset) throws Codec.ParseException {
        String s = input.subSequence(offset, input.length()).toString().trim();
        try {
            return new Codec.ParsingResult<>(parseInterval(s), input.length());
        } catch (Exception e) {
            throw new Codec.ParseException(input, offset, "Invalid interval: " + s + " (" + e.getMessage() + ")");
        }
    }

    @Override
    public byte[] encode(Interval value) {
        // Binary format: int64 microseconds, int32 days, int32 months
        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
        buf.putLong(value.microseconds());
        buf.putInt(value.days());
        buf.putInt(value.months());
        return buf.array();
    }

    @Override
    public Interval decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        if (length != 16) throw new Codec.ParseException("Binary interval must be 16 bytes, got " + length);
        long micros = buf.getLong();
        int days = buf.getInt();
        int months = buf.getInt();
        return new Interval(months, days, micros);
    }

    // -----------------------------------------------------------------------
    // ISO 8601 "with designators" format, e.g. P1Y2M3DT4H5M6S
    // -----------------------------------------------------------------------

    static String intervalToIso8601(Interval v) {
        int months = v.months();
        int days = v.days();
        long micros = v.microseconds();

        // Signs are kept per-component
        int absMonths = Math.abs(months);
        int absDays = Math.abs(days);
        long absMicros = Math.abs(micros);

        int years = absMonths / 12;
        int mons = absMonths % 12;

        long totalSecMicros = absMicros;
        long hours = totalSecMicros / (3_600_000_000L);
        totalSecMicros %= 3_600_000_000L;
        long mins = totalSecMicros / 60_000_000L;
        totalSecMicros %= 60_000_000L;
        long secs = totalSecMicros / 1_000_000L;
        long fracMicros = totalSecMicros % 1_000_000L;

        String mSign = months < 0 ? "-" : "";
        String dSign = days < 0 ? "-" : "";
        String tSign = micros < 0 ? "-" : "";

        StringBuilder sb = new StringBuilder("P");
        if (years != 0)  sb.append(mSign).append(years).append('Y');
        if (mons != 0)   sb.append(mSign).append(mons).append('M');
        if (absDays != 0) sb.append(dSign).append(absDays).append('D');
        if (hours != 0 || mins != 0 || secs != 0 || fracMicros != 0) {
            sb.append('T');
            if (hours != 0) sb.append(tSign).append(hours).append('H');
            if (mins != 0)  sb.append(tSign).append(mins).append('M');
            if (secs != 0 || fracMicros != 0) {
                sb.append(tSign).append(secs);
                if (fracMicros != 0) {
                    sb.append('.').append(String.format("%06d", fracMicros).replaceAll("0+$", ""));
                }
                sb.append('S');
            }
        }
        if (sb.length() == 1) sb.append("T0S"); // zero interval
        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // PostgreSQL "postgres" style interval parser
    // Handles both postgres style ("1 year 2 mons 03:04:05") and ISO 8601 ("P1Y2MT3H")
    // -----------------------------------------------------------------------

    static Interval parseInterval(String s) throws Exception {
        if (s.startsWith("P") || s.startsWith("-P") || s.startsWith("+P")) {
            return parseIso8601(s);
        }
        return parsePostgresStyle(s);
    }

    /**
     * Parses PostgreSQL's default "postgres" interval style, e.g.:
     * {@code "1 year 2 mons 3 days 04:05:06.789"}
     */
    private static Interval parsePostgresStyle(String s) throws Exception {
        int months = 0, days = 0;
        long microseconds = 0;
        String[] tokens = s.trim().split("\\s+");
        int i = 0;
        while (i < tokens.length) {
            String tok = tokens[i];
            // Could be a time part like "04:05:06" or "-04:05:06"
            String timePart = tok.startsWith("-") ? tok.substring(1) : tok;
            boolean timeNeg = tok.startsWith("-");
            if (timePart.contains(":")) {
                long sign = timeNeg ? -1L : 1L;
                String[] tp = timePart.split(":");
                long h = Long.parseLong(tp[0]);
                long m = Long.parseLong(tp[1]);
                double secFrac = tp.length > 2 ? Double.parseDouble(tp[2]) : 0.0;
                long s2 = (long) secFrac;
                long frac = Math.round((secFrac - s2) * 1_000_000);
                microseconds += sign * (h * 3_600_000_000L + m * 60_000_000L + s2 * 1_000_000L + frac);
                i++;
                continue;
            }
            // Numeric value followed by a unit
            if (i + 1 >= tokens.length) break;
            long num = Long.parseLong(tok);
            String unit = tokens[i + 1].toLowerCase();
            switch (unit) {
                case "year", "years"   -> months += (int)(num * 12);
                case "mon", "mons",
                     "month", "months" -> months += (int)num;
                case "day", "days"     -> days += (int)num;
                case "hour", "hours"   -> microseconds += num * 3_600_000_000L;
                case "min", "mins",
                     "minute","minutes"-> microseconds += num * 60_000_000L;
                case "sec", "secs",
                     "second","seconds"-> microseconds += num * 1_000_000L;
                default -> throw new Exception("Unknown interval unit: " + unit);
            }
            i += 2;
        }
        return new Interval(months, days, microseconds);
    }

    /**
     * Parses ISO 8601 interval format, e.g. {@code P1Y2M3DT4H5M6.5S}.
     */
    private static Interval parseIso8601(String s) throws Exception {
        boolean neg = s.startsWith("-");
        String iso = neg ? s.substring(1) : s;
        if (!iso.startsWith("P")) throw new Exception("ISO 8601 interval must start with P");
        iso = iso.substring(1);

        int months = 0, days = 0;
        long microseconds = 0;

        int tIdx = iso.indexOf('T');
        String datePart = tIdx >= 0 ? iso.substring(0, tIdx) : iso;
        String timePart = tIdx >= 0 ? iso.substring(tIdx + 1) : "";

        if (!datePart.isEmpty()) {
            // Parse: [nY][nM][nD]
            java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("(-?\\d+)([YMD])")
                    .matcher(datePart);
            while (m.find()) {
                long n = Long.parseLong(m.group(1));
                switch (m.group(2)) {
                    case "Y" -> months += (int)(n * 12);
                    case "M" -> months += (int)n;
                    case "D" -> days += (int)n;
                }
            }
        }
        if (!timePart.isEmpty()) {
            // Parse: [nH][nM][n[.f]S]
            java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("(-?\\d+(?:\\.\\d+)?)([HMS])")
                    .matcher(timePart);
            while (m.find()) {
                String numStr = m.group(1);
                switch (m.group(2)) {
                    case "H" -> microseconds += Long.parseLong(numStr) * 3_600_000_000L;
                    case "M" -> microseconds += Long.parseLong(numStr) * 60_000_000L;
                    case "S" -> {
                        double sec = Double.parseDouble(numStr);
                        long s2 = (long) sec;
                        long frac = Math.round((sec - s2) * 1_000_000);
                        microseconds += s2 * 1_000_000L + frac;
                    }
                }
            }
        }
        if (neg) { months = -months; days = -days; microseconds = -microseconds; }
        return new Interval(months, days, microseconds);
    }

}
