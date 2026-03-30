package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Random;

/** Codec for PostgreSQL {@code timestamptz} values, represented as {@link Instant}. */
final class TimestamptzCodec implements Codec<Instant> {

  @Override
  public String name() {
    return "timestamptz";
  }

  @Override
  public int scalarOid() {
    return 1184;
  }

  @Override
  public int arrayOid() {
    return 1185;
  }

  @Override
  public void write(StringBuilder sb, Instant value) {
    long pgMicros = toPgMicros(value);
    TimestampCodec.writeTimestamp(sb, pgMicros);
    sb.append("+00");
  }

  @Override
  public Codec.ParsingResult<Instant> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      long pgMicros = TimestampCodec.parseTimestamp(s);
      return new Codec.ParsingResult<>(fromPgMicros(pgMicros), input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid timestamptz: " + s);
    }
  }

  @Override
  public void encodeInBinary(Instant value, ByteArrayOutputStream out) {
    long v = toPgMicros(value);
    out.write((int) (v >>> 56) & 0xFF);
    out.write((int) (v >>> 48) & 0xFF);
    out.write((int) (v >>> 40) & 0xFF);
    out.write((int) (v >>> 32) & 0xFF);
    out.write((int) (v >>> 24) & 0xFF);
    out.write((int) (v >>> 16) & 0xFF);
    out.write((int) (v >>> 8) & 0xFF);
    out.write((int) (v & 0xFF));
  }

  @Override
  public Instant decodeInBinary(ByteBuffer buf, int length) {
    long pgMicros = buf.getLong();
    return fromPgMicros(pgMicros);
  }

  @Override
  public Instant random(Random r, int size) {
    if (size == 0) return Instant.ofEpochSecond(TimestampCodec.PG_EPOCH_UNIX_SECONDS);
    long bound = (long) size * 86_400_000_000L;
    long pgMicros = r.nextLong(-bound, bound + 1);
    return fromPgMicros(pgMicros);
  }

  /** Converts an {@link Instant} to PG microseconds from 2000-01-01 UTC. */
  static long toPgMicros(Instant instant) {
    long unixMicros = instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    return unixMicros - TimestampCodec.PG_EPOCH_UNIX_MICROS;
  }

  /** Converts PG microseconds from 2000-01-01 UTC to an {@link Instant}. */
  static Instant fromPgMicros(long pgMicros) {
    long unixMicros = pgMicros + TimestampCodec.PG_EPOCH_UNIX_MICROS;
    long epochSecond = Math.floorDiv(unixMicros, 1_000_000L);
    long microOfSecond = Math.floorMod(unixMicros, 1_000_000L);
    return Instant.ofEpochSecond(epochSecond, microOfSecond * 1_000L);
  }
}
