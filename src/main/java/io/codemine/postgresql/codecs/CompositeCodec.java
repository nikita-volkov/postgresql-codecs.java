package io.codemine.postgresql.codecs;

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
 *   int32  field_oid    (OID of the field type; 0 if not statically known)
 *   int32  field_length (-1 for NULL)
 *   byte[] field_data   (only present when field_length != -1)
 * </pre>
 *
 * @param <Z> the composite type
 */
public final class CompositeCodec<Z> implements Codec<Z> {

  private final String schema;
  private final String pgName;
  private final Function<Object[], Z> constructor;
  private final Field<Z, ?>[] fields;

  /**
   * Creates a composite codec for any number of fields.
   *
   * <p>The {@code construct} function receives the decoded field values as an {@code Object[]} in
   * the same order as the supplied {@code fields}, and must return the composite value. Each
   * element may be {@code null} when the corresponding PostgreSQL field is NULL.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * new CompositeCodec<>(
   *     "",
   *     "my_point",
   *     args -> new Point((Integer) args[0], (Integer) args[1]),
   *     new CompositeCodec.Field<>("x", Point::x, Codec.INT4),
   *     new CompositeCodec.Field<>("y", Point::y, Codec.INT4))
   * }</pre>
   *
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct function receiving decoded field values and returning the composite object
   * @param fields field descriptors (at least one)
   */
  @SafeVarargs
  @SuppressWarnings("unchecked")
  public CompositeCodec(
      String schema, String name, Function<Object[], Z> construct, Field<Z, ?>... fields) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields = fields;
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
  public void encodeInText(StringBuilder sb, Z value) {
    sb.append('(');
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      var field = (Field<Z, Object>) fields[i];
      Object fieldValue = field.accessor.apply(value);
      if (fieldValue != null) {
        var fieldSb = new StringBuilder();
        field.codec.encodeInText(fieldSb, fieldValue);
        int len = fieldSb.length();
        if (len == 0) {
          sb.append("\"\"");
        } else {
          boolean needsQuoting = false;
          for (int j = 0; j < len; j++) {
            char c = fieldSb.charAt(j);
            if (c == ',' || c == '(' || c == ')' || c == '"' || c == '\\' || c == ' ' || c == '\t'
                || c == '\n' || c == '\r') {
              needsQuoting = true;
              break;
            }
          }
          if (!needsQuoting) {
            sb.append(fieldSb);
          } else {
            sb.append('"');
            for (int j = 0; j < len; j++) {
              char c = fieldSb.charAt(j);
              switch (c) {
                case '"' -> sb.append("\"\"");
                case '\\' -> sb.append("\\\\");
                default -> sb.append(c);
              }
            }
            sb.append('"');
          }
        }
      }
    }
    sb.append(')');
  }

  @Override
  @SuppressWarnings("unchecked")
  public Codec.ParsingResult<Z> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    int len = input.length();
    if (offset >= len || input.charAt(offset) != '(') {
      throw new Codec.DecodingException(input, offset, "Expected '(' to open composite " + pgName);
    }
    int i = offset + 1; // skip '('
    Object[] args = new Object[fields.length];
    for (int fieldIdx = 0; fieldIdx < fields.length; fieldIdx++) {
      if (fieldIdx > 0) {
        if (i >= len || input.charAt(i) != ',') {
          throw new Codec.DecodingException(
              input, i, "Expected ',' between fields in composite " + pgName);
        }
        i++; // skip ','
      }
      if (i >= len || input.charAt(i) == ',' || input.charAt(i) == ')') {
        // NULL field
        args[fieldIdx] = null;
      } else if (input.charAt(i) == '"') {
        // Quoted field — unescape into a StringBuilder and parse it directly
        i++; // skip opening '"'
        var sb = new StringBuilder();
        quoteLoop:
        while (i < len) {
          char c = input.charAt(i);
          switch (c) {
            case '"' -> {
              if (i + 1 < len && input.charAt(i + 1) == '"') {
                sb.append('"');
                i += 2;
              } else {
                i++; // skip closing '"'
                break quoteLoop;
              }
            }
            case '\\' -> {
              if (i + 1 < len) {
                sb.append(input.charAt(i + 1));
                i += 2;
              } else {
                sb.append(c);
                i++;
              }
            }
            default -> {
              sb.append(c);
              i++;
            }
          }
        }
        args[fieldIdx] = ((Codec<Object>) fields[fieldIdx].codec).decodeInText(sb, 0).value;
      } else {
        // Unquoted field — pass a subSequence bounded to this field
        int fieldStart = i;
        while (i < len && input.charAt(i) != ',' && input.charAt(i) != ')') {
          i++;
        }
        args[fieldIdx] =
            ((Codec<Object>) fields[fieldIdx].codec)
                .decodeInText(input.subSequence(fieldStart, i), 0)
                .value;
      }
    }
    if (i >= len || input.charAt(i) != ')') {
      throw new Codec.DecodingException(input, i, "Expected ')' to close composite " + pgName);
    }
    return new Codec.ParsingResult<>(constructor.apply(args), i + 1);
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
   *   int32  field_oid    (OID of the field type; 0 if not statically known)
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
      writeInt32(out, field.codec.oid());
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
  public Z decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    if (length < 4) {
      throw new Codec.DecodingException("Binary composite too short: " + length);
    }
    int fieldCount = buf.getInt();
    if (fieldCount != fields.length) {
      throw new Codec.DecodingException(
          "Binary composite field count mismatch: expected "
              + fields.length
              + ", got "
              + fieldCount);
    }

    Object[] args = new Object[fields.length];
    for (int idx = 0; idx < fields.length; idx++) {
      var field = (Field<Z, Object>) fields[idx];
      int fieldOid = buf.getInt();
      int expectedFieldOid = field.codec.oid();
      if (expectedFieldOid != 0 && fieldOid != expectedFieldOid) {
        throw new Codec.DecodingException(
            "Unexpected field OID in composite binary decode for field '"
                + field.name
                + "' of "
                + pgName
                + ": expected "
                + expectedFieldOid
                + ", got "
                + fieldOid);
      }
      int fieldLen = buf.getInt();
      args[idx] =
          fieldLen == -1 ? null : ((Codec<Object>) field.codec).decodeInBinary(buf, fieldLen);
    }
    return constructor.apply(args);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Z random(Random r, int size) {
    Object[] args = new Object[fields.length];
    for (int idx = 0; idx < fields.length; idx++) {
      args[idx] = ((Codec<Object>) fields[idx].codec).random(r, size);
    }
    return constructor.apply(args);
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
