package io.pgenie.postgresqlcodecs.codecs;

import io.pgenie.postgresqlcodecs.types.Bit;
import io.pgenie.postgresqlcodecs.types.Cidr;
import io.pgenie.postgresqlcodecs.types.Inet;
import io.pgenie.postgresqlcodecs.types.Interval;
import io.pgenie.postgresqlcodecs.types.Macaddr;
import io.pgenie.postgresqlcodecs.types.Macaddr8;
import io.pgenie.postgresqlcodecs.types.Tsvector;
import io.pgenie.postgresqlcodecs.types.Varbit;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;

/**
 * Arbitrary value generators for each PostgreSQL codec type.
 *
 * <p>Each static method returns a {@link Stream} of {@link Arguments} suitable
 * for use with JUnit 5's {@code @MethodSource}, producing {@value #COUNT}
 * randomly-generated values that cover the full valid range of the
 * corresponding PostgreSQL type.
 *
 * <p>Generation logic for complex types (network, bit-string, interval, tsvector)
 * is delegated to the associated type class in the {@code main} source tree so
 * that downstream users can reuse the same generators in their own property tests.
 *
 * <p>Analogous to the {@code Arbitrary} instances in the Haskell
 * <a href="https://github.com/nikita-volkov/postgresql-types">postgresql-types</a>
 * library.
 */
public final class Generators {

    /** Number of random samples per property test (mirrors QuickCheck's default). */
    static final int COUNT = 100;

    // PostgreSQL date range: 4713 BC (= proleptic year -4712) to 5874897 AD.
    private static final long DATE_MIN_EPOCH_DAY = LocalDate.of(-4712, 1, 1).toEpochDay();
    private static final long DATE_MAX_EPOCH_DAY = LocalDate.of(5874897, 12, 31).toEpochDay();
    // Lower bound for AD-only generators (avoids JDBC binding limitations for BC dates).
    private static final long DATE_AD_EPOCH_DAY = LocalDate.of(1, 1, 1).toEpochDay();

    // PostgreSQL timestamp range: 4713 BC to 294276 AD (stored as microseconds since PG epoch).
    private static final long TIMESTAMP_MAX_EPOCH_DAY = LocalDate.of(294276, 12, 31).toEpochDay();

    private Generators() {
    }

    // -----------------------------------------------------------------------
    // Boolean
    // -----------------------------------------------------------------------

    /** Arbitrary {@code bool} values. */
    public static Stream<Arguments> booleans() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(r.nextBoolean())).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // Integer types
    // -----------------------------------------------------------------------

    /** Arbitrary {@code int2} values covering the full [-32768, 32767] range. */
    public static Stream<Arguments> int2s() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of((short) r.nextInt(Short.MIN_VALUE, (int) Short.MAX_VALUE + 1))).limit(COUNT);
    }

    /** Arbitrary {@code int4} values covering the full 32-bit signed integer range. */
    public static Stream<Arguments> int4s() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(r.nextInt())).limit(COUNT);
    }

    /** Arbitrary {@code int8} values covering the full 64-bit signed integer range. */
    public static Stream<Arguments> int8s() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(r.nextLong())).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // Floating-point types (finite values only for equality-based round-trips)
    // -----------------------------------------------------------------------

    /**
     * Arbitrary finite {@code float4} values.
     *
     * <p>NaN and ±Infinity are excluded because {@code NaN != NaN} under
     * {@link Float#equals}; the existing fixed tests cover those special values.
     */
    public static Stream<Arguments> float4s() {
        var r = new Random();
        return Stream.generate(() -> {
            float v;
            do {
                v = Float.intBitsToFloat(r.nextInt());
            } while (!Float.isFinite(v));
            return Arguments.of(v);
        }).limit(COUNT);
    }

    /**
     * Arbitrary finite {@code float8} values.
     *
     * <p>NaN and ±Infinity are excluded because {@code NaN != NaN} under
     * {@link Double#equals}; the existing fixed tests cover those special values.
     */
    public static Stream<Arguments> float8s() {
        var r = new Random();
        return Stream.generate(() -> {
            double v;
            do {
                v = Double.longBitsToDouble(r.nextLong());
            } while (!Double.isFinite(v));
            return Arguments.of(v);
        }).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // Numeric
    // -----------------------------------------------------------------------

    /**
     * Arbitrary {@code numeric} values with varying precision and scale.
     *
     * <p>Covers: zero, small integers, large integers, fractional values,
     * and negative values.
     */
    public static Stream<Arguments> numerics() {
        var r = new Random();
        return Stream.generate(() -> {
            boolean negative = r.nextBoolean();
            int intDigits = r.nextInt(0, 10);
            int fracDigits = r.nextInt(0, 7);
            var sb = new StringBuilder();
            if (negative) sb.append('-');
            for (int i = 0; i < intDigits; i++) {
                sb.append((char) ('0' + r.nextInt(0, 10)));
            }
            if (intDigits == 0) sb.append('0');
            if (fracDigits > 0) {
                sb.append('.');
                for (int i = 0; i < fracDigits; i++) {
                    sb.append((char) ('0' + r.nextInt(0, 10)));
                }
                // Ensure at least one non-zero fractional digit to preserve scale
                if (sb.toString().matches("-?0+\\.0+")) {
                    sb.setCharAt(sb.length() - 1, '1');
                }
            }
            return Arguments.of(new BigDecimal(sb.toString()));
        }).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // Text types (strings without NUL characters)
    // -----------------------------------------------------------------------

    /**
     * Arbitrary {@code text} values — Unicode strings that exclude the NUL
     * character ({@code '\0'}), which PostgreSQL does not allow in text.
     */
    public static Stream<Arguments> texts() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(arbitraryText(r, 50))).limit(COUNT);
    }

    /** Arbitrary {@code varchar} values (same constraints as {@code text}). */
    public static Stream<Arguments> varchars() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(arbitraryText(r, 50))).limit(COUNT);
    }

    /**
     * Arbitrary {@code char(1)} values — single printable ASCII characters.
     *
     * <p>PostgreSQL {@code char} is blank-padded; a single-character string
     * is the natural unit here.
     */
    public static Stream<Arguments> chars() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(String.valueOf((char) ('!' + r.nextInt(0, '~' - '!' + 1))))).limit(COUNT);
    }

    private static String arbitraryText(Random r, int maxLen) {
        int len = r.nextInt(0, maxLen + 1);
        var sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c;
            do {
                // Generate printable Unicode BMP characters excluding surrogates and NUL.
                c = (char) (1 + r.nextInt(0, 0xD7FF));
            } while (c == '\0');
            sb.append(c);
        }
        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // Bytea
    // -----------------------------------------------------------------------

    /** Arbitrary {@code bytea} values (random byte arrays). */
    public static Stream<Arguments> byteas() {
        var r = new Random();
        return Stream.generate(() -> {
            byte[] bytes = new byte[r.nextInt(0, 101)];
            r.nextBytes(bytes);
            return Arguments.of((Object) bytes);
        }).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // Date / time types
    // -----------------------------------------------------------------------

    /**
     * Arbitrary {@code date} values spanning the full PostgreSQL range (4713 BC to 5874897 AD).
     *
     * <p>Use for binary round-trip tests. For text round-trip tests use {@link #datesAD()}
     * which restricts to AD dates to avoid JDBC binding limitations for BC dates.
     */
    public static Stream<Arguments> dates() {
        var r = new Random();
        return Stream.generate(() -> {
            long day = DATE_MIN_EPOCH_DAY + (long) (r.nextDouble() * (DATE_MAX_EPOCH_DAY - DATE_MIN_EPOCH_DAY));
            return Arguments.of(LocalDate.ofEpochDay(day));
        }).limit(COUNT);
    }

    /**
     * Arbitrary AD-only {@code date} values (year 1 AD to 5874897 AD).
     *
     * <p>Restricts to AD dates to avoid JDBC binding issues with BC dates in
     * {@code ps.setDate(Date.valueOf(bcDate))}. Use for text round-trip tests.
     */
    public static Stream<Arguments> datesAD() {
        var r = new Random();
        return Stream.generate(() -> {
            long day = DATE_AD_EPOCH_DAY + (long) (r.nextDouble() * (DATE_MAX_EPOCH_DAY - DATE_AD_EPOCH_DAY));
            return Arguments.of(LocalDate.ofEpochDay(day));
        }).limit(COUNT);
    }

    /**
     * Arbitrary {@code time} values with microsecond precision
     * ({@code 00:00:00} to {@code 23:59:59.999999}).
     */
    public static Stream<Arguments> times() {
        var r = new Random();
        return Stream.generate(() -> {
            long micros = (long) (r.nextDouble() * 86_399_999_999L);
            return Arguments.of(LocalTime.ofNanoOfDay(micros * 1_000L));
        }).limit(COUNT);
    }

    /**
     * Arbitrary {@code timetz} values: a local time with microsecond precision
     * paired with a UTC offset in the range [−15:00:00, +15:00:00].
     */
    public static Stream<Arguments> timetzes() {
        var r = new Random();
        return Stream.generate(() -> {
            long micros = (long) (r.nextDouble() * 86_399_999_999L);
            LocalTime lt = LocalTime.ofNanoOfDay(micros * 1_000L);
            // Offsets must be whole seconds; range ±54000 s (= ±15 h).
            int tzSecs = r.nextInt(-54_000, 54_001);
            ZoneOffset tz = ZoneOffset.ofTotalSeconds(tzSecs);
            return Arguments.of(lt.atOffset(tz));
        }).limit(COUNT);
    }

    /**
     * Arbitrary {@code timestamp} values spanning PostgreSQL's documented range:
     * 4713 BC to 294276 AD, with microsecond precision.
     *
     * <p>Use for binary round-trip tests. For text round-trip tests use
     * {@link #timestampsAD()} which restricts to AD dates.
     */
    public static Stream<Arguments> timestamps() {
        var r = new Random();
        return Stream.generate(() -> {
            long epochDay = DATE_MIN_EPOCH_DAY
                    + (long) (r.nextDouble() * (TIMESTAMP_MAX_EPOCH_DAY - DATE_MIN_EPOCH_DAY));
            long micros = (long) (r.nextDouble() * 86_399_999_999L);
            LocalDate date = LocalDate.ofEpochDay(epochDay);
            LocalTime time = LocalTime.ofNanoOfDay(micros * 1_000L);
            return Arguments.of(LocalDateTime.of(date, time));
        }).limit(COUNT);
    }

    /**
     * Arbitrary AD-only {@code timestamp} values (year 1 AD to 294276 AD, microsecond precision).
     *
     * <p>Restricts to AD dates to avoid JDBC binding issues with BC dates in
     * {@code ps.setTimestamp(Timestamp.valueOf(bcDateTime))}. Use for text round-trip tests.
     */
    public static Stream<Arguments> timestampsAD() {
        var r = new Random();
        return Stream.generate(() -> {
            long epochDay = DATE_AD_EPOCH_DAY
                    + (long) (r.nextDouble() * (TIMESTAMP_MAX_EPOCH_DAY - DATE_AD_EPOCH_DAY));
            long micros = (long) (r.nextDouble() * 86_399_999_999L);
            LocalDate date = LocalDate.ofEpochDay(epochDay);
            LocalTime time = LocalTime.ofNanoOfDay(micros * 1_000L);
            return Arguments.of(LocalDateTime.of(date, time));
        }).limit(COUNT);
    }

    /**
     * Arbitrary UTC {@code timestamptz} values spanning PostgreSQL's full range.
     *
     * <p>Use for binary round-trip tests. For text round-trip tests use
     * {@link #timestamptzADs()} which restricts to AD dates.
     */
    public static Stream<Arguments> timestamptzs() {
        var r = new Random();
        return Stream.generate(() -> {
            long epochDay = DATE_MIN_EPOCH_DAY
                    + (long) (r.nextDouble() * (TIMESTAMP_MAX_EPOCH_DAY - DATE_MIN_EPOCH_DAY));
            long micros = (long) (r.nextDouble() * 86_399_999_999L);
            LocalDate date = LocalDate.ofEpochDay(epochDay);
            LocalTime time = LocalTime.ofNanoOfDay(micros * 1_000L);
            return Arguments.of(LocalDateTime.of(date, time).atOffset(ZoneOffset.UTC));
        }).limit(COUNT);
    }

    /**
     * Arbitrary AD-only UTC {@code timestamptz} values (year 1 AD to 294276 AD).
     *
     * <p>Restricts to AD dates to avoid JDBC binding issues. Use for text round-trip tests.
     */
    public static Stream<Arguments> timestamptzADs() {
        var r = new Random();
        return Stream.generate(() -> {
            long epochDay = DATE_AD_EPOCH_DAY
                    + (long) (r.nextDouble() * (TIMESTAMP_MAX_EPOCH_DAY - DATE_AD_EPOCH_DAY));
            long micros = (long) (r.nextDouble() * 86_399_999_999L);
            LocalDate date = LocalDate.ofEpochDay(epochDay);
            LocalTime time = LocalTime.ofNanoOfDay(micros * 1_000L);
            return Arguments.of(LocalDateTime.of(date, time).atOffset(ZoneOffset.UTC));
        }).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // UUID
    // -----------------------------------------------------------------------

    /** Arbitrary {@code uuid} values (random 128-bit UUIDs). */
    public static Stream<Arguments> uuids() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(new UUID(r.nextLong(), r.nextLong()))).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // OID
    // -----------------------------------------------------------------------

    /** Arbitrary {@code oid} values in the PostgreSQL valid range [0, 2³²−1]. */
    public static Stream<Arguments> oids() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(r.nextLong(0L, 4_294_967_296L))).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // Network address types — generation logic delegated to main-code types
    // -----------------------------------------------------------------------

    /**
     * Arbitrary {@code inet} values covering both IPv4 and IPv6 addresses.
     * Delegates to {@link Inet#generate(Random)}.
     */
    public static Stream<Arguments> inets() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(Inet.generate(r))).limit(COUNT);
    }

    /**
     * Arbitrary {@code cidr} values covering both IPv4 and IPv6 network prefixes.
     * Delegates to {@link Cidr#generate(Random)}.
     */
    public static Stream<Arguments> cidrs() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(Cidr.generate(r))).limit(COUNT);
    }

    /**
     * Arbitrary {@code macaddr} values.
     * Delegates to {@link Macaddr#generate(Random)}.
     */
    public static Stream<Arguments> macaddrs() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(Macaddr.generate(r))).limit(COUNT);
    }

    /**
     * Arbitrary {@code macaddr8} values.
     * Delegates to {@link Macaddr8#generate(Random)}.
     */
    public static Stream<Arguments> macaddr8s() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(Macaddr8.generate(r))).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // Geometric types
    // -----------------------------------------------------------------------

    /** Arbitrary {@code point} values. */
    public static Stream<Arguments> points() {
        var r = new Random();
        return Stream.generate(() ->
                Arguments.of(new PGpoint(r.nextDouble(-1e6, 1e6), r.nextDouble(-1e6, 1e6)))
        ).limit(COUNT);
    }

    /**
     * Arbitrary {@code line} values (infinite line described by {@code ax+by+c=0}).
     *
     * <p>At least one of {@code a} or {@code b} must be non-zero.
     */
    public static Stream<Arguments> lines() {
        var r = new Random();
        return Stream.generate(() -> {
            double a, b;
            do {
                a = r.nextDouble(-10.0, 10.0);
                b = r.nextDouble(-10.0, 10.0);
            } while (a == 0.0 && b == 0.0);
            double c = r.nextDouble(-10.0, 10.0);
            try {
                return Arguments.of(new PGline(a, b, c));
            } catch (Exception e) {
                return Arguments.of(new PGline(1.0, 0.0, 0.0));
            }
        }).limit(COUNT);
    }

    /** Arbitrary {@code lseg} values (line segments). */
    public static Stream<Arguments> lsegs() {
        var r = new Random();
        return Stream.generate(() -> {
            var p1 = new PGpoint(r.nextDouble(-1e6, 1e6), r.nextDouble(-1e6, 1e6));
            var p2 = new PGpoint(r.nextDouble(-1e6, 1e6), r.nextDouble(-1e6, 1e6));
            return Arguments.of(new PGlseg(p1, p2));
        }).limit(COUNT);
    }

    /** Arbitrary {@code box} values. */
    public static Stream<Arguments> boxes() {
        var r = new Random();
        return Stream.generate(() -> {
            double x1 = r.nextDouble(-1e6, 1e6), y1 = r.nextDouble(-1e6, 1e6);
            double x2 = r.nextDouble(-1e6, 1e6), y2 = r.nextDouble(-1e6, 1e6);
            var upper = new PGpoint(Math.max(x1, x2), Math.max(y1, y2));
            var lower = new PGpoint(Math.min(x1, x2), Math.min(y1, y2));
            return Arguments.of(new PGbox(upper, lower));
        }).limit(COUNT);
    }

    /**
     * Arbitrary {@code path} values (open or closed, 2–5 points).
     *
     * <p>Closed paths are analogous to PostgreSQL's closed-path syntax
     * {@code ((x1,y1),...)}; open paths use {@code [(x1,y1),...]}.
     */
    public static Stream<Arguments> paths() {
        var r = new Random();
        return Stream.generate(() -> {
            int n = r.nextInt(2, 6);
            boolean isOpen = r.nextBoolean();
            var pts = new PGpoint[n];
            for (int i = 0; i < n; i++) {
                pts[i] = new PGpoint(r.nextDouble(-1e4, 1e4), r.nextDouble(-1e4, 1e4));
            }
            return Arguments.of(new PGpath(pts, isOpen));
        }).limit(COUNT);
    }

    /**
     * Arbitrary {@code polygon} values (2–5 vertices).
     */
    public static Stream<Arguments> polygons() {
        var r = new Random();
        return Stream.generate(() -> {
            int n = r.nextInt(2, 6);
            var pts = new PGpoint[n];
            for (int i = 0; i < n; i++) {
                pts[i] = new PGpoint(r.nextDouble(-1e4, 1e4), r.nextDouble(-1e4, 1e4));
            }
            return Arguments.of(new PGpolygon(pts));
        }).limit(COUNT);
    }

    /**
     * Arbitrary {@code circle} values.
     *
     * <p>The radius is always non-negative, matching PostgreSQL's constraint
     * (and the Haskell {@code Circle} Arbitrary instance).
     */
    public static Stream<Arguments> circles() {
        var r = new Random();
        return Stream.generate(() -> {
            double cx = r.nextDouble(-1e6, 1e6);
            double cy = r.nextDouble(-1e6, 1e6);
            double radius = Math.abs(r.nextDouble(0, 1e6));
            var center = new PGpoint(cx, cy);
            return Arguments.of(new PGcircle(center, radius));
        }).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // Bit-string types — generation logic delegated to main-code types
    // -----------------------------------------------------------------------

    /**
     * Arbitrary {@code bit} values (fixed-length bit strings of 1–64 bits).
     * Delegates to {@link Bit#generate(Random)}.
     */
    public static Stream<Arguments> bits() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(Bit.generate(r))).limit(COUNT);
    }

    /**
     * Arbitrary {@code varbit} values (variable-length bit strings, 0–64 bits).
     * Delegates to {@link Varbit#generate(Random)}.
     */
    public static Stream<Arguments> varbits() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(Varbit.generate(r))).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // JSON types
    // -----------------------------------------------------------------------

    /**
     * Arbitrary {@code json} / {@code jsonb} values.
     *
     * <p>Generates simple well-formed JSON literals (objects, arrays, strings,
     * numbers, booleans, null) that are accepted by PostgreSQL.
     */
    public static Stream<Arguments> jsons() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(arbitraryJson(r))).limit(COUNT);
    }

    public static Stream<Arguments> jsonbs() {
        return jsons();
    }

    private static String arbitraryJson(Random r) {
        return switch (r.nextInt(0, 3)) {
            case 0 -> {
                // JSON object with 0–3 string keys
                int n = r.nextInt(0, 4);
                var sb = new StringBuilder("{");
                for (int i = 0; i < n; i++) {
                    if (i > 0) sb.append(',');
                    sb.append('"').append(arbitraryAlpha(r, 1, 8)).append('"');
                    sb.append(':');
                    sb.append(arbitraryJsonScalar(r));
                }
                sb.append('}');
                yield sb.toString();
            }
            case 1 -> {
                // JSON array with 0–3 elements
                int n = r.nextInt(0, 4);
                var sb = new StringBuilder("[");
                for (int i = 0; i < n; i++) {
                    if (i > 0) sb.append(',');
                    sb.append(arbitraryJsonScalar(r));
                }
                sb.append(']');
                yield sb.toString();
            }
            default -> arbitraryJsonScalar(r);
        };
    }

    private static String arbitraryJsonScalar(Random r) {
        return switch (r.nextInt(0, 4)) {
            case 0 -> '"' + arbitraryAlpha(r, 0, 10) + '"';
            case 1 -> String.valueOf(r.nextInt(-1000, 1001));
            case 2 -> r.nextBoolean() ? "true" : "false";
            default -> "null";
        };
    }

    private static String arbitraryAlpha(Random r, int minLen, int maxLen) {
        int len = r.nextInt(minLen, maxLen + 1);
        var sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + r.nextInt(0, 26)));
        }
        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // Tsvector — generation logic delegated to main-code type
    // -----------------------------------------------------------------------

    /**
     * Arbitrary {@code tsvector} values (position-free, 1–4 lowercase lexemes).
     * Delegates to {@link Tsvector#generate(Random)}.
     */
    public static Stream<Arguments> tsvectors() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(Tsvector.generate(r))).limit(COUNT);
    }

    // -----------------------------------------------------------------------
    // Interval — generation logic delegated to main-code type
    // -----------------------------------------------------------------------

    /**
     * Arbitrary {@code interval} values spanning the full PostgreSQL interval range.
     * Delegates to {@link Interval#generate(Random)}.
     */
    public static Stream<Arguments> intervals() {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(Interval.generate(r))).limit(COUNT);
    }

}
