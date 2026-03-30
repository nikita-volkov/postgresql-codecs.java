package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

/**
 * A codec for a single scalar value.
 *
 * <p>Each codec supports two wire formats:
 *
 * <ul>
 *   <li><b>Textual</b> — the PostgreSQL text representation, used via {@link #write} and {@link
 *       #parse}. These methods are also used when encoding composite and array fields inside a
 *       composite/array literal.
 *   <li><b>Binary</b> — the PostgreSQL binary wire format, used via {@link #encodeInBinary} and
 *       {@link #decodeInBinary}. Binary encoding is compact, unambiguous and required when
 *       assembling composite or array values in binary protocol mode.
 * </ul>
 *
 * @param <A> the type of the value
 */
public interface Codec<A> {

  Codec<Integer> INT4 = new Int4Codec();
  Codec<String> TEXT = new TextCodec();
  Codec<Inet> INET = new InetCodec();
  Codec<Macaddr> MACADDR = new MacaddrCodec();
  Codec<Boolean> BOOL = new BoolCodec();
  Codec<Short> INT2 = new Int2Codec();
  Codec<Long> INT8 = new Int8Codec();
  Codec<Float> FLOAT4 = new Float4Codec();
  Codec<Double> FLOAT8 = new Float8Codec();
  Codec<java.math.BigDecimal> NUMERIC = new NumericCodec();
  Codec<Bytea> BYTEA = new ByteaCodec();
  Codec<java.util.UUID> UUID = new UuidCodec();
  Codec<String> JSON = new JsonCodec();
  Codec<String> JSONB = new JsonbCodec();
  Codec<String> VARCHAR = new VarcharCodec();
  Codec<String> BPCHAR = new BpcharCodec();
  Codec<Byte> CHAR = new CharCodec();
  Codec<Integer> OID = new OidCodec();
  Codec<Long> MONEY = new MoneyCodec();
  Codec<Integer> DATE = new DateCodec();
  Codec<Long> TIME = new TimeCodec();
  Codec<Timetz> TIMETZ = new TimetzCodec();
  Codec<Long> TIMESTAMP = new TimestampCodec();
  Codec<Long> TIMESTAMPTZ = new TimestamptzCodec();
  Codec<Interval> INTERVAL = new IntervalCodec();
  Codec<Point> POINT = new PointCodec();
  Codec<Line> LINE = new LineCodec();
  Codec<Lseg> LSEG = new LsegCodec();
  Codec<Box> BOX = new BoxCodec();
  Codec<Path> PATH = new PathCodec();
  Codec<Polygon> POLYGON = new PolygonCodec();
  Codec<Circle> CIRCLE = new CircleCodec();
  Codec<Inet> CIDR = new CidrCodec();
  Codec<Macaddr8> MACADDR8 = new Macaddr8Codec();
  Codec<Bit> BIT = new BitCodec();
  Codec<Bit> VARBIT = new VarbitCodec();

  // -----------------------------------------------------------------------
  // Type metadata
  // -----------------------------------------------------------------------
  /**
   * Returns the PostgreSQL schema name for this type, or empty string or {@code null} if the type
   * is in the default search path.
   */
  default String schema() {
    return "";
  }

  /** Returns the PostgreSQL type name (e.g. {@code "int4"}, {@code "text"}). */
  String name();

  /**
   * Returns the number of array dimensions for this codec. {@code 0} for scalar codecs; {@code n}
   * for n-dimensional array codecs.
   */
  default int dimensions() {
    return 0;
  }

  /** Returns the full PostgreSQL type signature, including schema if applicable. */
  default String typeSig() {
    StringBuilder sb = new StringBuilder();
    if (schema() != null && !schema().isEmpty()) {
      sb.append(schema()).append(".");
    }
    sb.append(name());
    for (int i = 0; i < dimensions(); i++) {
      sb.append("[]");
    }

    return sb.toString();
  }

  /**
   * Returns an array codec whose element type is this codec. The returned codec uses PostgreSQL's
   * array literal syntax ({@code {elem1,elem2,...}}) for text format and the standard binary array
   * header for binary format.
   */
  default Codec<List<A>> inDim() {
    return new ArrayCodec<>(this);
  }

  /**
   * Returns the PostgreSQL base-type OID, or {@code 0} if not statically known (e.g. user-defined
   * composite types).
   *
   * <p>The OID is used inside binary-format array and composite headers to tag each element with
   * its type.
   */
  default int scalarOid() {
    return 0;
  }

  /** Returns the PostgreSQL array-type OID for this element type, or {@code 0} if not known. */
  default int arrayOid() {
    return 0;
  }

  default int oid() {
    return dimensions() > 0 ? arrayOid() : scalarOid();
  }

  default int jdbcType() {
    return java.sql.Types.OTHER;
  }

  // -----------------------------------------------------------------------
  // Textual wire format
  // -----------------------------------------------------------------------
  /**
   * Writes the given value to the string builder in PostgreSQL textual literal form.
   *
   * <p>This is primarily used for encoding fields inside composite and array literals. The written
   * form must be the canonical text representation accepted by PostgreSQL for the type.
   */
  void write(StringBuilder sb, A value);

  /**
   * Parses a PostgreSQL text-format literal of type A from {@code input} starting at {@code
   * offset}.
   *
   * <p>The input must be a non-null {@link CharSequence} holding the raw text as returned by the
   * PostgreSQL server (e.g. the string value of a column obtained via {@link
   * java.sql.ResultSet#getString}). Passing the {@code String} directly avoids an extra copy
   * compared to converting to a {@code char[]} first. NULL column values must be handled by the
   * caller before invoking this method.
   *
   * <p>Returns the parsed value together with the offset of the first character that was
   * <em>not</em> consumed, allowing callers to continue parsing subsequent fields without copying
   * the input. Throws {@link DecodingException} if the input cannot be interpreted as a valid
   * literal of type A.
   */
  ParsingResult<A> parse(CharSequence input, int offset) throws DecodingException;

  // -----------------------------------------------------------------------
  // Binary wire format
  // -----------------------------------------------------------------------
  /**
   * Encodes the given non-null value into the PostgreSQL binary wire format, appending the bytes
   * directly to {@code out}.
   *
   * <p>Appends exactly the binary payload for the value — no length prefix. The caller (e.g. {@link
   * CompositeCodec}) is responsible for prepending the 4-byte {@code int32} length header required
   * by the PostgreSQL composite and array binary protocols.
   *
   * <p>The byte order is always <b>big-endian</b>, as required by the PostgreSQL wire protocol.
   *
   * @throws UnsupportedOperationException if binary encoding is not implemented for this type
   */
  void encodeInBinary(A value, ByteArrayOutputStream out);

  /**
   * Convenience overload that encodes the value into a freshly-allocated byte array and returns it.
   * Delegates to {@link #encodeInBinary(Object, ByteArrayOutputStream)}.
   */
  default byte[] encodeInBinaryAsByteArray(A value) {
    var out = new ByteArrayOutputStream();
    encodeInBinary(value, out);
    return out.toByteArray();
  }

  /**
   * Convenience overload that encodes the value into a {@link ByteBuffer}. Delegates to {@link
   * #encodeInBinaryAsByteArray(Object)}.
   */
  default ByteBuffer encodeInBinaryAsByteBuffer(A value) {
    return ByteBuffer.wrap(encodeInBinaryAsByteArray(value));
  }

  /**
   * Decodes a value from the PostgreSQL binary wire format.
   *
   * <p>{@code buf} must be a big-endian {@link ByteBuffer} positioned at the first byte of the
   * value's payload. {@code length} is the number of bytes that make up the payload (as read from
   * the preceding {@code int32} length header). The method advances the buffer position by exactly
   * {@code length} bytes.
   *
   * <p>NULL handling ({@code length == -1}) must be performed by the caller before invoking this
   * method.
   *
   * @throws DecodingException if the binary data is malformed
   * @throws UnsupportedOperationException if binary decoding is not implemented for this type
   */
  A decodeInBinary(ByteBuffer buf, int length) throws DecodingException;

  /**
   * Generates a random value of type A, for testing purposes. The provided {@link Random} instance
   * should be used as the source of randomness, and the generated values should cover a wide range
   * of possible inputs, including edge cases.
   *
   * <p>The {@code size} parameter follows the QuickCheck convention: it is a non-negative integer
   * that controls the "size" of the generated value — for scalars it bounds the magnitude, for
   * collections it bounds the number of elements, and for multidimensional arrays it is propagated
   * uniformly to all sub-arrays so that the resulting array has a rectangular shape.
   */
  A random(Random r, int size);

  /**
   * Returns a new codec that maps values of type A to type B using the provided pair of mapping
   * functions. The returned codec encodes and decodes values of type B by delegating to this codec
   * for the underlying A values.
   *
   * @param <B> the target value type
   * @param to function mapping from A to B
   * @param from function mapping from B back to A
   * @return a new codec for values of type B
   */
  default <B> Codec<B> map(Function<A, B> to, Function<B, A> from) {
    return new MappedCodec<>(this, to, from);
  }

  // -----------------------------------------------------------------------
  // Result / exception types
  // -----------------------------------------------------------------------
  /** Holds the parsed value together with the offset of the first unconsumed character. */
  final class ParsingResult<A> {

    public final A value;
    public final int nextOffset;

    public ParsingResult(A value, int nextOffset) {
      this.value = value;
      this.nextOffset = nextOffset;
    }
  }

  /** Thrown when a text or binary value cannot be parsed into the expected type. */
  final class DecodingException extends Exception {

    public DecodingException(CharSequence input, int offset, String message) {
      this(input.subSequence(offset, input.length()), message);
    }

    public DecodingException(CharSequence input, String message) {
      super(String.format("Parse error: %s (input: \"%s\")", message, input));
    }

    public DecodingException(String message) {
      super(message);
    }
  }
}
