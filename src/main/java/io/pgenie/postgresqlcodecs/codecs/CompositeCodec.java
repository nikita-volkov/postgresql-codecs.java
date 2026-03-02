package io.pgenie.postgresqlcodecs.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.Function;

/**
 * Codec for PostgreSQL composite (row) types.
 *
 * <p>Supports both the PostgreSQL text literal format {@code (val1,val2,...)} and the binary
 * composite wire format.
 *
 * <p><b>Binary format</b>:
 *
 * <pre>
 * int32  field_count
 * [for each field]:
 *   int32  field_oid    (OID of the field; 0 for unknown/user-defined)
 *   int32  field_length (-1 for NULL)
 *   byte[] field_data   (only present when field_length != -1)
 * </pre>
 *
 * @param <Z> the composite type
 */
public final class CompositeCodec<Z> implements Codec<Z> {

  private final String schema;
  private final String pgName;
  private final Object constructor;
  private final Field<Z, ?>[] fields;

  /**
   * Creates a 2-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B> CompositeCodec(
      String schema,
      String name,
      Function<A, Function<B, Z>> construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields = new Field[] {fieldA, fieldB};
  }

  /**
   * Creates a 3-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C> CompositeCodec(
      String schema,
      String name,
      Function<A, Function<B, Function<C, Z>>> construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields = new Field[] {fieldA, fieldB, fieldC};
  }

  /**
   * Creates a 4-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D> CompositeCodec(
      String schema,
      String name,
      Function<A, Function<B, Function<C, Function<D, Z>>>> construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields = new Field[] {fieldA, fieldB, fieldC, fieldD};
  }

  /**
   * Creates a 5-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E> CompositeCodec(
      String schema,
      String name,
      Function<A, Function<B, Function<C, Function<D, Function<E, Z>>>>> construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields = new Field[] {fieldA, fieldB, fieldC, fieldD, fieldE};
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
  @SuppressWarnings("unchecked")
  public void write(StringBuilder sb, Z value) {
    sb.append('(');
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      var field = (Field<Z, Object>) fields[i];
      Object fieldValue = field.accessor.apply(value);
      if (fieldValue != null) {
        var fieldSb = new StringBuilder();
        field.codec.write(fieldSb, fieldValue);
        writeQuotedField(sb, fieldSb);
      }
    }
    sb.append(')');
  }

  @Override
  @SuppressWarnings("unchecked")
  public Codec.ParsingResult<Z> parse(CharSequence input, int offset) throws Codec.ParseException {
    int len = input.length();
    if (offset >= len || input.charAt(offset) != '(') {
      throw new Codec.ParseException(input, offset, "Expected '(' to open composite " + pgName);
    }
    int i = offset + 1; // skip '('
    Object fn = constructor;
    for (int fieldIdx = 0; fieldIdx < fields.length; fieldIdx++) {
      if (fieldIdx > 0) {
        if (i >= len || input.charAt(i) != ',') {
          throw new Codec.ParseException(
              input, i, "Expected ',' between fields in composite " + pgName);
        }
        i++; // skip ','
      }
      if (i >= len || input.charAt(i) == ',' || input.charAt(i) == ')') {
        // NULL field
        fn = ((Function<Object, Object>) fn).apply(null);
      } else if (input.charAt(i) == '"') {
        // Quoted field — unescape into a StringBuilder and parse it directly
        i++; // skip opening '"'
        var sb = new StringBuilder();
        while (i < len) {
          char c = input.charAt(i);
          if (c == '"') {
            if (i + 1 < len && input.charAt(i + 1) == '"') {
              sb.append('"');
              i += 2;
            } else {
              i++; // skip closing '"'
              break;
            }
          } else if (c == '\\') {
            if (i + 1 < len) {
              sb.append(input.charAt(i + 1));
              i += 2;
            } else {
              sb.append(c);
              i++;
            }
          } else {
            sb.append(c);
            i++;
          }
        }
        var result = ((Codec<Object>) fields[fieldIdx].codec).parse(sb, 0);
        fn = ((Function<Object, Object>) fn).apply(result.value);
      } else {
        // Unquoted field — pass a subSequence bounded to this field
        int fieldStart = i;
        while (i < len && input.charAt(i) != ',' && input.charAt(i) != ')') {
          i++;
        }
        var result =
            ((Codec<Object>) fields[fieldIdx].codec).parse(input.subSequence(fieldStart, i), 0);
        fn = ((Function<Object, Object>) fn).apply(result.value);
      }
    }
    if (i >= len || input.charAt(i) != ')') {
      throw new Codec.ParseException(input, i, "Expected ')' to close composite " + pgName);
    }
    return new Codec.ParsingResult<>((Z) fn, i + 1);
  }

  // -----------------------------------------------------------------------
  // Binary wire format
  // -----------------------------------------------------------------------
  /**
   * Encodes the composite value in the PostgreSQL binary composite format, appending the bytes to
   * {@code out}.
   *
   * <p>Layout:
   *
   * <pre>
   * int32  field_count
   * [for each field]:
   *   int32  field_oid    (0 for unknown OIDs)
   *   int32  field_length (-1 for NULL)
   *   byte[] field_data
   * </pre>
   */
  @Override
  @SuppressWarnings("unchecked")
  public void encodeInBinary(Z value, ByteArrayOutputStream out) {
    writeInt32(out, fields.length);
    for (var f : fields) {
      var field = (Field<Z, Object>) f;
      Object fieldValue = field.accessor.apply(value);
      writeInt32(out, field.codec.scalarOid());
      if (fieldValue == null) {
        writeInt32(out, -1);
      } else {
        var fieldOut = new ByteArrayOutputStream();
        field.codec.encodeInBinary(fieldValue, fieldOut);
        writeInt32(out, fieldOut.size());
        out.write(fieldOut.toByteArray(), 0, fieldOut.size());
      }
    }
  }

  private static void writeInt32(ByteArrayOutputStream out, int v) {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 8) & 0xFF);
    out.write(v & 0xFF);
  }

  /** Decodes a composite value from the PostgreSQL binary composite format. */
  @Override
  @SuppressWarnings("unchecked")
  public Z decodeInBinary(ByteBuffer buf, int length) throws Codec.ParseException {
    if (length < 4) {
      throw new Codec.ParseException("Binary composite too short: " + length);
    }
    int fieldCount = buf.getInt();
    if (fieldCount != fields.length) {
      throw new Codec.ParseException(
          "Binary composite field count mismatch: expected "
              + fields.length
              + ", got "
              + fieldCount);
    }

    Object fn = constructor;
    for (int i = 0; i < fields.length; i++) {
      int fieldOid = buf.getInt(); // OID — informational, not validated
      int fieldLen = buf.getInt();
      if (fieldLen == -1) {
        fn = ((Function<Object, Object>) fn).apply(null);
      } else {
        Object fieldValue = ((Codec<Object>) fields[i].codec).decodeInBinary(buf, fieldLen);
        fn = ((Function<Object, Object>) fn).apply(fieldValue);
      }
    }
    return (Z) fn;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Z random(Random r) {
    Object fn = constructor;
    for (var f : fields) {
      var field = (Field<Z, Object>) f;
      Object randomValue = ((Codec<Object>) field.codec).random(r);
      fn = ((Function<Object, Object>) fn).apply(randomValue);
    }
    return (Z) fn;
  }

  // -----------------------------------------------------------------------
  // row(...) helper
  // -----------------------------------------------------------------------
  /**
   * Writes the value in {@code row(...)} syntax, which handles nested composites better than the
   * quoted-literal form.
   */
  @SuppressWarnings("unchecked")
  public void writeAsRow(StringBuilder sb, Z value) {
    sb.append("row(");
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      var field = (Field<Z, Object>) fields[i];
      Object fieldValue = field.accessor.apply(value);
      if (fieldValue == null) {
        sb.append("null");
      } else if (field.codec instanceof CompositeCodec<?> compositeCodec) {
        @SuppressWarnings("rawtypes")
        var cc = (CompositeCodec) compositeCodec;
        cc.writeAsRow(sb, fieldValue);
      } else {
        var fieldSb = new StringBuilder();
        field.codec.write(fieldSb, fieldValue);
        writeRowLiteral(sb, fieldSb);
      }
    }
    sb.append(')');
  }

  // ---------------------------------------------------------------------------
  // PostgreSQL composite text format helpers
  // ---------------------------------------------------------------------------
  private static void writeQuotedField(StringBuilder sb, StringBuilder fieldText) {
    int len = fieldText.length();
    if (len == 0) {
      sb.append("\"\"");
      return;
    }
    boolean needsQuoting = false;
    for (int i = 0; i < len; i++) {
      char c = fieldText.charAt(i);
      if (c == ',' || c == '(' || c == ')' || c == '"' || c == '\\' || c == ' ' || c == '\t'
          || c == '\n' || c == '\r') {
        needsQuoting = true;
        break;
      }
    }
    if (!needsQuoting) {
      sb.append(fieldText);
      return;
    }
    sb.append('"');
    for (int i = 0; i < len; i++) {
      char c = fieldText.charAt(i);
      if (c == '"') {
        sb.append("\"\"");
      } else if (c == '\\') {
        sb.append("\\\\");
      } else {
        sb.append(c);
      }
    }
    sb.append('"');
  }

  /**
   * Writes a scalar literal in the row(...) syntax context. Single-quotes the value and escapes
   * embedded single quotes by doubling them.
   */
  private static void writeRowLiteral(StringBuilder sb, StringBuilder fieldText) {
    sb.append('\'');
    int len = fieldText.length();
    for (int i = 0; i < len; i++) {
      char c = fieldText.charAt(i);
      if (c == '\'') {
        sb.append("''");
      } else {
        sb.append(c);
      }
    }
    sb.append('\'');
  }

  /**
   * Describes a single field inside a PostgreSQL composite type.
   *
   * @param <Z> the composite type
   * @param <A> the field value type
   */
  public static final class Field<Z, A> {

    public final String name;
    public final Function<Z, A> accessor;
    public final Codec<A> codec;

    /**
     * Creates a new field descriptor.
     *
     * @param name the PostgreSQL column name
     * @param accessor function extracting this field's value from the composite
     * @param codec codec used to encode/decode this field's values
     */
    public Field(String name, Function<Z, A> accessor, Codec<A> codec) {
      this.name = name;
      this.accessor = accessor;
      this.codec = codec;
    }
  }
}
