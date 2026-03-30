package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Random;

/** Codec for PostgreSQL {@code date} values, represented as {@link LocalDate}. */
final class DateCodec implements Codec<LocalDate> {

  private static final LocalDate PG_EPOCH = LocalDate.of(2000, 1, 1);
  private static final long PG_EPOCH_DAY = PG_EPOCH.toEpochDay();

  @Override
  public String name() {
    return "date";
  }

  @Override
  public int scalarOid() {
    return 1082;
  }

  @Override
  public int arrayOid() {
    return 1182;
  }

  @Override
  public void write(StringBuilder sb, LocalDate value) {
    sb.append(value);
  }

  @Override
  public Codec.ParsingResult<LocalDate> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    String s = input.subSequence(offset, input.length()).toString().trim();
    try {
      LocalDate date = LocalDate.parse(s);
      return new Codec.ParsingResult<>(date, input.length());
    } catch (Exception e) {
      throw new Codec.DecodingException(input, offset, "Invalid date: " + s);
    }
  }

  @Override
  public void encodeInBinary(LocalDate value, ByteArrayOutputStream out) {
    int days = (int) (value.toEpochDay() - PG_EPOCH_DAY);
    out.write((days >>> 24) & 0xFF);
    out.write((days >>> 16) & 0xFF);
    out.write((days >>> 8) & 0xFF);
    out.write(days & 0xFF);
  }

  @Override
  public LocalDate decodeInBinary(ByteBuffer buf, int length) {
    int days = buf.getInt();
    return LocalDate.ofEpochDay(PG_EPOCH_DAY + days);
  }

  @Override
  public LocalDate random(Random r, int size) {
    if (size == 0) return PG_EPOCH;
    int bound = Math.min(size * 365, 3_652_425);
    int offset = r.nextInt(2 * bound + 1) - bound;
    return PG_EPOCH.plusDays(offset);
  }
}
