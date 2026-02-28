package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
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
 *   <li><b>Binary</b> — the PostgreSQL binary wire format, used via {@link #encode} and {@link
 *       #decodeBinary}. Binary encoding is compact, unambiguous and required when assembling
 *       composite or array values in binary protocol mode.
 * </ul>
 *
 * @param <A> the type of the value
 */
public interface Codec<A> {

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

  /** Returns the full PostgreSQL type signature, including schema if applicable. */
  default String typeSig() {
    String schema = schema();
    return schema == null || schema.isEmpty() ? name() : schema + "." + name();
  }

  /**
   * Returns the PostgreSQL base-type OID, or {@code 0} if not statically known (e.g. user-defined
   * composite types).
   *
   * <p>The OID is used inside binary-format array and composite headers to tag each element with
   * its type.
   */
  default int oid() {
    return 0;
  }

  /** Returns the PostgreSQL array-type OID for this element type, or {@code 0} if not known. */
  default int arrayOid() {
    return 0;
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
   * the input. Throws {@link ParseException} if the input cannot be interpreted as a valid literal
   * of type A.
   */
  ParsingResult<A> parse(CharSequence input, int offset) throws ParseException;

  // -----------------------------------------------------------------------
  // Binary wire format
  // -----------------------------------------------------------------------
  /**
   * Encodes the given non-null value into the PostgreSQL binary wire format.
   *
   * <p>The returned byte array holds exactly the binary payload for the value — no length prefix.
   * The caller (e.g. {@link ArrayCodec} or {@link CompositeCodec}) is responsible for prepending
   * the 4-byte {@code int32} length header required by the PostgreSQL composite and array binary
   * protocols.
   *
   * <p>The byte order is always <b>big-endian</b>, as required by the PostgreSQL wire protocol.
   *
   * @throws UnsupportedOperationException if binary encoding is not implemented for this type
   */
  byte[] encode(A value);

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
   * @throws ParseException if the binary data is malformed
   * @throws UnsupportedOperationException if binary decoding is not implemented for this type
   */
  A decodeBinary(ByteBuffer buf, int length) throws ParseException;

  /**
   * Returns a new codec that maps values of type A to type B using the provided pair of mapping
   * functions. The returned codec encodes and decodes values of type B by delegating to this codec
   * for the underlying A values.
   *
   * @param <B>
   * @param to
   * @param from
   * @return
   */
  default <B> Codec<B> map(Function<A, B> to, Function<B, A> from) {
    return new MappedCodec<>(this, to, from);
  }

  // -----------------------------------------------------------------------
  // Result / exception types
  // -----------------------------------------------------------------------
  final class ParsingResult<A> {

    public final A value;
    public final int nextOffset;

    public ParsingResult(A value, int nextOffset) {
      this.value = value;
      this.nextOffset = nextOffset;
    }
  }

  final class ParseException extends Exception {

    public ParseException(CharSequence input, int offset, String message) {
      this(input.subSequence(offset, input.length()), message);
    }

    public ParseException(CharSequence input, String message) {
      super(String.format("Parse error: %s (input: \"%s\")", message, input));
    }

    public ParseException(String message) {
      super(message);
    }
  }
}
