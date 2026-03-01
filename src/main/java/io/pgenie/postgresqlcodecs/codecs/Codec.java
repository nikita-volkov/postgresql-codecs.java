package io.pgenie.postgresqlcodecs.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
   * Returns a new 1-D array codec whose element type is this codec. The returned codec uses
   * PostgreSQL's array literal syntax ({@code {elem1,elem2,...}}) for text format and the standard
   * binary array header for binary format.
   */
  default Codec<List<A>> inDim() {
    return new Codec<>() {

      @Override
      public String name() {
        return Codec.this.name();
      }

      @Override
      public int dimensions() {
        return Codec.this.dimensions() + 1;
      }

      @Override
      public int oid() {
        return Codec.this.arrayOid();
      }

      // -----------------------------------------------------------------------
      // Textual wire format
      // -----------------------------------------------------------------------

      @Override
      public void write(StringBuilder sb, List<A> value) {
        sb.append("{");
        for (int i = 0; i < value.size(); i++) {
          if (i > 0) {
            sb.append(",");
          }
          Codec.this.write(sb, value.get(i));
        }
        sb.append("}");
      }

      /**
       * Parses a PostgreSQL 1-D array literal ({@code {elem1,elem2,...}}) from {@code input}
       * starting at {@code offset}. Handles double-quoted elements (with backslash escaping) and
       * bare elements.
       */
      @Override
      public ParsingResult<List<A>> parse(CharSequence input, int offset) throws ParseException {
        if (offset >= input.length() || input.charAt(offset) != '{') {
          throw new ParseException(input, offset, "Expected '{' to open array literal");
        }
        int pos = offset + 1;
        List<A> list = new ArrayList<>();

        // Empty array
        if (pos < input.length() && input.charAt(pos) == '}') {
          return new ParsingResult<>(list, pos + 1);
        }

        while (pos < input.length()) {
          char c = input.charAt(pos);

          if (c == '"') {
            // Quoted element: collect raw chars between double-quotes, honouring backslash escapes.
            pos++; // skip opening '"'
            StringBuilder elem = new StringBuilder();
            while (pos < input.length()) {
              char ec = input.charAt(pos);
              if (ec == '\\' && pos + 1 < input.length()) {
                elem.append(input.charAt(pos + 1));
                pos += 2;
              } else if (ec == '"') {
                break;
              } else {
                elem.append(ec);
                pos++;
              }
            }
            if (pos >= input.length() || input.charAt(pos) != '"') {
              throw new ParseException(input, offset, "Unterminated quoted array element");
            }
            pos++; // skip closing '"'
            list.add(Codec.this.parse(elem.toString(), 0).value);
          } else {
            // Bare element: read until ',' or '}'.
            int start = pos;
            while (pos < input.length() && input.charAt(pos) != ',' && input.charAt(pos) != '}') {
              pos++;
            }
            String elemStr = input.subSequence(start, pos).toString();
            list.add(Codec.this.parse(elemStr, 0).value);
          }

          if (pos >= input.length()) {
            throw new ParseException(input, offset, "Unexpected end of input parsing array");
          }
          char sep = input.charAt(pos);
          if (sep == '}') {
            return new ParsingResult<>(list, pos + 1);
          } else if (sep == ',') {
            pos++;
          } else {
            throw new ParseException(
                input, offset, "Expected ',' or '}' in array literal, got '" + sep + "'");
          }
        }
        throw new ParseException(input, offset, "Unexpected end of input parsing array");
      }

      // -----------------------------------------------------------------------
      // Binary wire format
      // -----------------------------------------------------------------------

      /**
       * Encodes a 1-D array in PostgreSQL binary array format.
       *
       * <pre>
       *   int32  ndim        = 1
       *   int32  flags       = 0
       *   int32  element_oid
       *   int32  dim_size
       *   int32  dim_lbound  = 1
       *   for each element:
       *     int32  elem_length  (-1 for NULL)
       *     byte[] elem_data
       * </pre>
       */
      @Override
      public void encode(List<A> value, ByteArrayOutputStream out) {
        writeInt32(out, 1); // ndim
        writeInt32(out, 0); // flags
        writeInt32(out, Codec.this.oid()); // element OID
        writeInt32(out, value.size()); // dimension size
        writeInt32(out, 1); // lower bound (PostgreSQL convention: 1-based)
        for (A elem : value) {
          if (elem == null) {
            writeInt32(out, -1);
          } else {
            ByteArrayOutputStream elemOut = new ByteArrayOutputStream();
            Codec.this.encode(elem, elemOut);
            byte[] bytes = elemOut.toByteArray();
            writeInt32(out, bytes.length);
            out.writeBytes(bytes);
          }
        }
      }

      /**
       * Decodes a PostgreSQL binary array. Expects ndim == 1 (or 0 for empty); multi-dimensional
       * arrays are rejected.
       */
      @Override
      public List<A> decodeBinary(ByteBuffer buf, int length) throws ParseException {
        int ndim = buf.getInt();
        buf.getInt(); // flags (ignored)
        buf.getInt(); // element OID (ignored; we trust the connection)
        if (ndim == 0) {
          return new ArrayList<>();
        }
        if (ndim != 1) {
          throw new ParseException(
              "Expected 1-dimensional array in binary decode, got ndim=" + ndim);
        }
        int dimSize = buf.getInt();
        buf.getInt(); // lower bound (ignored)
        List<A> result = new ArrayList<>(dimSize);
        for (int i = 0; i < dimSize; i++) {
          int elemLen = buf.getInt();
          if (elemLen == -1) {
            result.add(null);
          } else {
            result.add(Codec.this.decodeBinary(buf, elemLen));
          }
        }
        return result;
      }

      // -----------------------------------------------------------------------
      // Random generation
      // -----------------------------------------------------------------------

      @Override
      public List<A> random(Random r) {
        int size = r.nextInt(6); // 0–5 elements
        List<A> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          list.add(Codec.this.random(r));
        }
        return list;
      }

      // -----------------------------------------------------------------------
      // Helper
      // -----------------------------------------------------------------------

      private void writeInt32(ByteArrayOutputStream out, int v) {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
      }
    };
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
  void encode(A value, ByteArrayOutputStream out);

  /**
   * Convenience overload that encodes the value into a freshly-allocated byte array and returns it.
   * Delegates to {@link #encode(Object, ByteArrayOutputStream)}.
   */
  default byte[] encode(A value) {
    var out = new ByteArrayOutputStream();
    encode(value, out);
    return out.toByteArray();
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
   * @throws ParseException if the binary data is malformed
   * @throws UnsupportedOperationException if binary decoding is not implemented for this type
   */
  A decodeBinary(ByteBuffer buf, int length) throws ParseException;

  /**
   * Generates a random value of type A, for testing purposes. The provided {@link Random} instance
   * should be used as the source of randomness, and the generated values should cover a wide range
   * of possible inputs, including edge cases.
   */
  A random(Random r);

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
