package io.pgenie.postgresqlcodecs.arbitrary;

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

import io.pgenie.postgresqlcodecs.types.Bit;
import io.pgenie.postgresqlcodecs.types.Cidr;
import io.pgenie.postgresqlcodecs.types.Inet;
import io.pgenie.postgresqlcodecs.types.Interval;
import io.pgenie.postgresqlcodecs.types.Macaddr;
import io.pgenie.postgresqlcodecs.types.Macaddr8;
import io.pgenie.postgresqlcodecs.types.Tsvector;
import io.pgenie.postgresqlcodecs.types.Varbit;

public interface Arbitrary<A> {

    int COUNT = 100;

    long DATE_MIN_EPOCH_DAY = LocalDate.of(-4712, 1, 1).toEpochDay();
    long DATE_MAX_EPOCH_DAY = LocalDate.of(5874897, 12, 31).toEpochDay();
    long DATE_AD_EPOCH_DAY = LocalDate.of(1, 1, 1).toEpochDay();
    long TIMESTAMP_MAX_EPOCH_DAY = LocalDate.of(294276, 12, 31).toEpochDay();

    A generate(Random randomizer);

    static <A> Stream<Arguments> samples(Arbitrary<A> arbitrary) {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(arbitrary.generate(r))).limit(COUNT);
    }

    static Stream<Arguments> booleans() { return samples(BOOLEAN); }

    static Stream<Arguments> int2s() { return samples(INT2); }

    static Stream<Arguments> int4s() { return samples(INT4); }

    static Stream<Arguments> int8s() { return samples(INT8); }

    static Stream<Arguments> float4s() { return samples(FLOAT4); }

    static Stream<Arguments> float8s() { return samples(FLOAT8); }

    static Stream<Arguments> numerics() { return samples(NUMERIC); }

    static Stream<Arguments> texts() { return samples(TEXT); }

    static Stream<Arguments> varchars() { return samples(VARCHAR); }

    static Stream<Arguments> chars() { return samples(CHAR); }

    static Stream<Arguments> byteas() { return samples(BYTEA); }

    static Stream<Arguments> dates() { return samples(DATE); }

    static Stream<Arguments> datesAD() { return samples(DATE_AD); }

    static Stream<Arguments> times() { return samples(TIME); }

    static Stream<Arguments> timetzes() { return samples(TIMETZ); }

    static Stream<Arguments> timestamps() { return samples(TIMESTAMP); }

    static Stream<Arguments> timestampsAD() { return samples(TIMESTAMP_AD); }

    static Stream<Arguments> timestamptzs() { return samples(TIMESTAMPTZ); }

    static Stream<Arguments> timestamptzADs() { return samples(TIMESTAMPTZ_AD); }

    static Stream<Arguments> uuids() { return samples(UUID_ARBITRARY); }

    static Stream<Arguments> oids() { return samples(OID); }

    static Stream<Arguments> inets() { return samples(INET); }

    static Stream<Arguments> cidrs() { return samples(CIDR); }

    static Stream<Arguments> macaddrs() { return samples(MACADDR); }

    static Stream<Arguments> macaddr8s() { return samples(MACADDR8); }

    static Stream<Arguments> points() { return samples(POINT); }

    static Stream<Arguments> lines() { return samples(LINE); }

    static Stream<Arguments> lsegs() { return samples(LSEG); }

    static Stream<Arguments> boxes() { return samples(BOX); }

    static Stream<Arguments> paths() { return samples(PATH); }

    static Stream<Arguments> polygons() { return samples(POLYGON); }

    static Stream<Arguments> circles() { return samples(CIRCLE); }

    static Stream<Arguments> bits() { return samples(BIT); }

    static Stream<Arguments> varbits() { return samples(VARBIT); }

    static Stream<Arguments> jsons() { return samples(JSON); }

    static Stream<Arguments> jsonbs() { return samples(JSONB); }

    static Stream<Arguments> tsvectors() { return samples(TSVECTOR); }

    static Stream<Arguments> intervals() { return samples(INTERVAL); }

    Arbitrary<Bit> BIT = Bit::generate;

    Arbitrary<Varbit> VARBIT = Varbit::generate;

    Arbitrary<Cidr> CIDR = Cidr::generate;

    Arbitrary<Inet> INET = Inet::generate;

    Arbitrary<Boolean> BOOLEAN = Random::nextBoolean;

    Arbitrary<Short> INT2 = r -> (short) r.nextInt(Short.MIN_VALUE, (int) Short.MAX_VALUE + 1);

    Arbitrary<Integer> INT4 = Random::nextInt;

    Arbitrary<Long> INT8 = Random::nextLong;

    Arbitrary<Float> FLOAT4 = r -> {
        float value;
        do {
            value = Float.intBitsToFloat(r.nextInt());
        } while (!Float.isFinite(value));
        return value;
    };

    Arbitrary<Double> FLOAT8 = r -> {
        double value;
        do {
            value = Double.longBitsToDouble(r.nextLong());
        } while (!Double.isFinite(value));
        return value;
    };

    Arbitrary<BigDecimal> NUMERIC = Arbitrary::arbitraryNumeric;

    Arbitrary<String> TEXT = r -> arbitraryText(r, 50);

    Arbitrary<String> VARCHAR = r -> arbitraryText(r, 50);

    Arbitrary<String> CHAR = r -> String.valueOf((char) ('!' + r.nextInt(0, '~' - '!' + 1)));

    Arbitrary<byte[]> BYTEA = r -> {
        byte[] bytes = new byte[r.nextInt(0, 101)];
        r.nextBytes(bytes);
        return bytes;
    };

    Arbitrary<LocalDate> DATE = r -> {
        long day = DATE_MIN_EPOCH_DAY + (long) (r.nextDouble() * (DATE_MAX_EPOCH_DAY - DATE_MIN_EPOCH_DAY));
        return LocalDate.ofEpochDay(day);
    };

    Arbitrary<LocalDate> DATE_AD = r -> {
        long day = DATE_AD_EPOCH_DAY + (long) (r.nextDouble() * (DATE_MAX_EPOCH_DAY - DATE_AD_EPOCH_DAY));
        return LocalDate.ofEpochDay(day);
    };

    Arbitrary<LocalTime> TIME = r -> {
        long micros = (long) (r.nextDouble() * 86_399_999_999L);
        return LocalTime.ofNanoOfDay(micros * 1_000L);
    };

    Arbitrary<OffsetTime> TIMETZ = r -> {
        long micros = (long) (r.nextDouble() * 86_399_999_999L);
        LocalTime lt = LocalTime.ofNanoOfDay(micros * 1_000L);
        int tzSecs = r.nextInt(-54_000, 54_001);
        ZoneOffset tz = ZoneOffset.ofTotalSeconds(tzSecs);
        return lt.atOffset(tz);
    };

    Arbitrary<LocalDateTime> TIMESTAMP = r -> {
        long epochDay = DATE_MIN_EPOCH_DAY
                + (long) (r.nextDouble() * (TIMESTAMP_MAX_EPOCH_DAY - DATE_MIN_EPOCH_DAY));
        long micros = (long) (r.nextDouble() * 86_399_999_999L);
        LocalDate date = LocalDate.ofEpochDay(epochDay);
        LocalTime time = LocalTime.ofNanoOfDay(micros * 1_000L);
        return LocalDateTime.of(date, time);
    };

    Arbitrary<LocalDateTime> TIMESTAMP_AD = r -> {
        long epochDay = DATE_AD_EPOCH_DAY
                + (long) (r.nextDouble() * (TIMESTAMP_MAX_EPOCH_DAY - DATE_AD_EPOCH_DAY));
        long micros = (long) (r.nextDouble() * 86_399_999_999L);
        LocalDate date = LocalDate.ofEpochDay(epochDay);
        LocalTime time = LocalTime.ofNanoOfDay(micros * 1_000L);
        return LocalDateTime.of(date, time);
    };

    Arbitrary<OffsetDateTime> TIMESTAMPTZ = r -> TIMESTAMP.generate(r).atOffset(ZoneOffset.UTC);

    Arbitrary<OffsetDateTime> TIMESTAMPTZ_AD = r -> TIMESTAMP_AD.generate(r).atOffset(ZoneOffset.UTC);

    Arbitrary<UUID> UUID_ARBITRARY = r -> new UUID(r.nextLong(), r.nextLong());

    Arbitrary<Long> OID = r -> r.nextLong(0L, 4_294_967_296L);

    Arbitrary<Macaddr> MACADDR = Macaddr::generate;

    Arbitrary<Macaddr8> MACADDR8 = Macaddr8::generate;

    Arbitrary<PGpoint> POINT = r -> new PGpoint(r.nextDouble(-1e6, 1e6), r.nextDouble(-1e6, 1e6));

    Arbitrary<PGline> LINE = r -> {
        double a;
        double b;
        do {
            a = r.nextDouble(-10.0, 10.0);
            b = r.nextDouble(-10.0, 10.0);
        } while (a == 0.0 && b == 0.0);
        double c = r.nextDouble(-10.0, 10.0);
        try {
            return new PGline(a, b, c);
        } catch (Exception ignored) {
            return new PGline(1.0, 0.0, 0.0);
        }
    };

    Arbitrary<PGlseg> LSEG = r -> {
        var p1 = new PGpoint(r.nextDouble(-1e6, 1e6), r.nextDouble(-1e6, 1e6));
        var p2 = new PGpoint(r.nextDouble(-1e6, 1e6), r.nextDouble(-1e6, 1e6));
        return new PGlseg(p1, p2);
    };

    Arbitrary<PGbox> BOX = r -> {
        double x1 = r.nextDouble(-1e6, 1e6);
        double y1 = r.nextDouble(-1e6, 1e6);
        double x2 = r.nextDouble(-1e6, 1e6);
        double y2 = r.nextDouble(-1e6, 1e6);
        var upper = new PGpoint(Math.max(x1, x2), Math.max(y1, y2));
        var lower = new PGpoint(Math.min(x1, x2), Math.min(y1, y2));
        return new PGbox(upper, lower);
    };

    Arbitrary<PGpath> PATH = r -> {
        int n = r.nextInt(2, 6);
        boolean isOpen = r.nextBoolean();
        var pts = new PGpoint[n];
        for (int i = 0; i < n; i++) {
            pts[i] = new PGpoint(r.nextDouble(-1e4, 1e4), r.nextDouble(-1e4, 1e4));
        }
        return new PGpath(pts, isOpen);
    };

    Arbitrary<PGpolygon> POLYGON = r -> {
        int n = r.nextInt(2, 6);
        var pts = new PGpoint[n];
        for (int i = 0; i < n; i++) {
            pts[i] = new PGpoint(r.nextDouble(-1e4, 1e4), r.nextDouble(-1e4, 1e4));
        }
        return new PGpolygon(pts);
    };

    Arbitrary<PGcircle> CIRCLE = r -> {
        double cx = r.nextDouble(-1e6, 1e6);
        double cy = r.nextDouble(-1e6, 1e6);
        double radius = Math.abs(r.nextDouble(0, 1e6));
        var center = new PGpoint(cx, cy);
        return new PGcircle(center, radius);
    };

    Arbitrary<String> JSON = Arbitrary::arbitraryJson;

    Arbitrary<String> JSONB = JSON;

    Arbitrary<Tsvector> TSVECTOR = Tsvector::generate;

    Arbitrary<Interval> INTERVAL = Interval::generate;

    private static BigDecimal arbitraryNumeric(Random r) {
        boolean negative = r.nextBoolean();
        int intDigits = r.nextInt(0, 10);
        int fracDigits = r.nextInt(0, 7);
        var sb = new StringBuilder();
        if (negative) {
            sb.append('-');
        }
        for (int i = 0; i < intDigits; i++) {
            sb.append((char) ('0' + r.nextInt(0, 10)));
        }
        if (intDigits == 0) {
            sb.append('0');
        }
        if (fracDigits > 0) {
            sb.append('.');
            for (int i = 0; i < fracDigits; i++) {
                sb.append((char) ('0' + r.nextInt(0, 10)));
            }
            if (sb.toString().matches("-?0+\\.0+")) {
                sb.setCharAt(sb.length() - 1, '1');
            }
        }
        return new BigDecimal(sb.toString());
    }

    private static String arbitraryText(Random r, int maxLen) {
        int len = r.nextInt(0, maxLen + 1);
        var sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c;
            do {
                c = (char) (1 + r.nextInt(0, 0xD7FF));
            } while (c == '\0');
            sb.append(c);
        }
        return sb.toString();
    }

    private static String arbitraryJson(Random r) {
        return switch (r.nextInt(0, 3)) {
            case 0 -> {
                int n = r.nextInt(0, 4);
                var sb = new StringBuilder("{");
                for (int i = 0; i < n; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append('"').append(arbitraryAlpha(r, 1, 8)).append('"');
                    sb.append(':');
                    sb.append(arbitraryJsonScalar(r));
                }
                sb.append('}');
                yield sb.toString();
            }
            case 1 -> {
                int n = r.nextInt(0, 4);
                var sb = new StringBuilder("[");
                for (int i = 0; i < n; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
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

}
