package io.pgenie.postgresqlCodecs.codecs;

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
public record Interval(int months, int days, long microseconds) {}
