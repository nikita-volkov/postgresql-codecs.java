package io.pgenie.postgresqlCodecs.types;

import java.util.Random;

/**
 * PostgreSQL {@code interval} type. Time span with separate month, day, and microsecond
 * components, each with their own sign.
 *
 * <p>Range: approximately −178,000,000 years to 178,000,000 years.
 *
 * <p>Port of the Haskell {@code PostgresqlTypes.Interval} record type.
 *
 * @param months       Number of months (may be negative).
 * @param days         Number of days (may be negative).
 * @param microseconds Number of microseconds (may be negative).
 */
public record Interval(int months, int days, long microseconds) {

    /**
     * Generates a random {@code Interval} spanning PostgreSQL's full range:
     * months in ±2,147,483,647, days in ±2,147,483,647, and microseconds in
     * approximately ±9.2×10¹⁸ (the full {@code int64} range).
     */
    public static Interval generate(Random r) {
        return new Interval(r.nextInt(), r.nextInt(), r.nextLong());
    }

}
