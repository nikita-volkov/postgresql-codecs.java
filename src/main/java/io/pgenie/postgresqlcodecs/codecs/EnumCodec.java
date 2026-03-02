package io.pgenie.postgresqlcodecs.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Codec for PostgreSQL {@code ENUM} types, mapping between Java enum constants and their
 * corresponding PostgreSQL label strings.
 *
 * @param <E> the Java enum type
 */
public final class EnumCodec<E> implements Codec<E> {

  private final String schema;
  private final String pgName;
  private final Map<E, String> pgLabels;
  private final Map<String, E> byPgLabel;

  /**
   * Creates a new codec for the PostgreSQL enum type {@code name} in {@code schema}.
   *
   * @param schema the PostgreSQL schema containing the enum type, or empty/null for the default
   *     search path
   * @param name the PostgreSQL enum type name
   * @param pgLabels mapping from each Java enum constant to its PostgreSQL label string
   */
  public EnumCodec(String schema, String name, Map<E, String> pgLabels) {
    this.schema = schema;
    this.pgName = name;
    this.pgLabels = pgLabels;
    this.byPgLabel = new HashMap<>(pgLabels.size() * 2);
    pgLabels.forEach((constant, label) -> byPgLabel.put(label, constant));
  }

  @Override
  public String schema() {
    return schema;
  }

  @Override
  public String name() {
    return pgName;
  }

  @Override
  public void write(StringBuilder sb, E value) {
    sb.append(pgLabels.get(value));
  }

  @Override
  public Codec.ParsingResult<E> parse(CharSequence input, int offset) throws Codec.ParseException {
    String label = input.subSequence(offset, input.length()).toString();
    E value = byPgLabel.get(label);
    if (value == null) {
      throw new Codec.ParseException(input, offset, "Unknown " + pgName + " value: " + label);
    }
    return new Codec.ParsingResult<>(value, input.length());
  }

  @Override
  public void encodeInBinary(E value, ByteArrayOutputStream out) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'encodeInBinary'");
  }

  @Override
  public E decodeInBinary(ByteBuffer buf, int length) throws ParseException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'decodeInBinary'");
  }

  @Override
  public E random(Random r) {
    List<E> values = new ArrayList<>(pgLabels.keySet());
    return values.get(r.nextInt(values.size()));
  }
}
