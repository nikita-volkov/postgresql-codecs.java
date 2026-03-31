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
  private final Object constructor;
  private final Field<Z, ?>[] fields;
  private final boolean vararg;

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
    this.vararg = false;
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
    this.vararg = false;
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
    this.vararg = false;
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
    this.vararg = false;
  }

  /**
   * Creates a 6-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param <F> type of the sixth field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   * @param fieldF sixth field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E, F> CompositeCodec(
      String schema,
      String name,
      Function<A, Function<B, Function<C, Function<D, Function<E, Function<F, Z>>>>>> construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE,
      Field<Z, F> fieldF) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields = new Field[] {fieldA, fieldB, fieldC, fieldD, fieldE, fieldF};
    this.vararg = false;
  }

  /**
   * Creates a 7-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param <F> type of the sixth field
   * @param <G> type of the seventh field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   * @param fieldF sixth field descriptor
   * @param fieldG seventh field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E, F, G> CompositeCodec(
      String schema,
      String name,
      Function<A, Function<B, Function<C, Function<D, Function<E, Function<F, Function<G, Z>>>>>>>
          construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE,
      Field<Z, F> fieldF,
      Field<Z, G> fieldG) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields = new Field[] {fieldA, fieldB, fieldC, fieldD, fieldE, fieldF, fieldG};
    this.vararg = false;
  }

  /**
   * Creates an 8-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param <F> type of the sixth field
   * @param <G> type of the seventh field
   * @param <H> type of the eighth field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   * @param fieldF sixth field descriptor
   * @param fieldG seventh field descriptor
   * @param fieldH eighth field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E, F, G, H> CompositeCodec(
      String schema,
      String name,
      Function<
              A,
              Function<
                  B,
                  Function<C, Function<D, Function<E, Function<F, Function<G, Function<H, Z>>>>>>>>
          construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE,
      Field<Z, F> fieldF,
      Field<Z, G> fieldG,
      Field<Z, H> fieldH) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields = new Field[] {fieldA, fieldB, fieldC, fieldD, fieldE, fieldF, fieldG, fieldH};
    this.vararg = false;
  }

  /**
   * Creates a 9-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param <F> type of the sixth field
   * @param <G> type of the seventh field
   * @param <H> type of the eighth field
   * @param <I> type of the ninth field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   * @param fieldF sixth field descriptor
   * @param fieldG seventh field descriptor
   * @param fieldH eighth field descriptor
   * @param fieldI ninth field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E, F, G, H, I> CompositeCodec(
      String schema,
      String name,
      Function<
              A,
              Function<
                  B,
                  Function<
                      C,
                      Function<
                          D, Function<E, Function<F, Function<G, Function<H, Function<I, Z>>>>>>>>>
          construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE,
      Field<Z, F> fieldF,
      Field<Z, G> fieldG,
      Field<Z, H> fieldH,
      Field<Z, I> fieldI) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields =
        new Field[] {fieldA, fieldB, fieldC, fieldD, fieldE, fieldF, fieldG, fieldH, fieldI};
    this.vararg = false;
  }

  /**
   * Creates a 10-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param <F> type of the sixth field
   * @param <G> type of the seventh field
   * @param <H> type of the eighth field
   * @param <I> type of the ninth field
   * @param <J> type of the tenth field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   * @param fieldF sixth field descriptor
   * @param fieldG seventh field descriptor
   * @param fieldH eighth field descriptor
   * @param fieldI ninth field descriptor
   * @param fieldJ tenth field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E, F, G, H, I, J> CompositeCodec(
      String schema,
      String name,
      Function<
              A,
              Function<
                  B,
                  Function<
                      C,
                      Function<
                          D,
                          Function<
                              E,
                              Function<
                                  F, Function<G, Function<H, Function<I, Function<J, Z>>>>>>>>>>
          construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE,
      Field<Z, F> fieldF,
      Field<Z, G> fieldG,
      Field<Z, H> fieldH,
      Field<Z, I> fieldI,
      Field<Z, J> fieldJ) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields =
        new Field[] {
          fieldA, fieldB, fieldC, fieldD, fieldE, fieldF, fieldG, fieldH, fieldI, fieldJ
        };
    this.vararg = false;
  }

  /**
   * Creates an 11-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param <F> type of the sixth field
   * @param <G> type of the seventh field
   * @param <H> type of the eighth field
   * @param <I> type of the ninth field
   * @param <J> type of the tenth field
   * @param <K> type of the eleventh field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   * @param fieldF sixth field descriptor
   * @param fieldG seventh field descriptor
   * @param fieldH eighth field descriptor
   * @param fieldI ninth field descriptor
   * @param fieldJ tenth field descriptor
   * @param fieldK eleventh field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E, F, G, H, I, J, K> CompositeCodec(
      String schema,
      String name,
      Function<
              A,
              Function<
                  B,
                  Function<
                      C,
                      Function<
                          D,
                          Function<
                              E,
                              Function<
                                  F,
                                  Function<
                                      G,
                                      Function<H, Function<I, Function<J, Function<K, Z>>>>>>>>>>>
          construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE,
      Field<Z, F> fieldF,
      Field<Z, G> fieldG,
      Field<Z, H> fieldH,
      Field<Z, I> fieldI,
      Field<Z, J> fieldJ,
      Field<Z, K> fieldK) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields =
        new Field[] {
          fieldA, fieldB, fieldC, fieldD, fieldE, fieldF, fieldG, fieldH, fieldI, fieldJ, fieldK
        };
    this.vararg = false;
  }

  /**
   * Creates a 12-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param <F> type of the sixth field
   * @param <G> type of the seventh field
   * @param <H> type of the eighth field
   * @param <I> type of the ninth field
   * @param <J> type of the tenth field
   * @param <K> type of the eleventh field
   * @param <L> type of the twelfth field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   * @param fieldF sixth field descriptor
   * @param fieldG seventh field descriptor
   * @param fieldH eighth field descriptor
   * @param fieldI ninth field descriptor
   * @param fieldJ tenth field descriptor
   * @param fieldK eleventh field descriptor
   * @param fieldL twelfth field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E, F, G, H, I, J, K, L> CompositeCodec(
      String schema,
      String name,
      Function<
              A,
              Function<
                  B,
                  Function<
                      C,
                      Function<
                          D,
                          Function<
                              E,
                              Function<
                                  F,
                                  Function<
                                      G,
                                      Function<
                                          H,
                                          Function<
                                              I, Function<J, Function<K, Function<L, Z>>>>>>>>>>>>
          construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE,
      Field<Z, F> fieldF,
      Field<Z, G> fieldG,
      Field<Z, H> fieldH,
      Field<Z, I> fieldI,
      Field<Z, J> fieldJ,
      Field<Z, K> fieldK,
      Field<Z, L> fieldL) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields =
        new Field[] {
          fieldA, fieldB, fieldC, fieldD, fieldE, fieldF, fieldG, fieldH, fieldI, fieldJ, fieldK,
          fieldL
        };
    this.vararg = false;
  }

  /**
   * Creates a 13-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param <F> type of the sixth field
   * @param <G> type of the seventh field
   * @param <H> type of the eighth field
   * @param <I> type of the ninth field
   * @param <J> type of the tenth field
   * @param <K> type of the eleventh field
   * @param <L> type of the twelfth field
   * @param <M> type of the thirteenth field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   * @param fieldF sixth field descriptor
   * @param fieldG seventh field descriptor
   * @param fieldH eighth field descriptor
   * @param fieldI ninth field descriptor
   * @param fieldJ tenth field descriptor
   * @param fieldK eleventh field descriptor
   * @param fieldL twelfth field descriptor
   * @param fieldM thirteenth field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E, F, G, H, I, J, K, L, M> CompositeCodec(
      String schema,
      String name,
      Function<
              A,
              Function<
                  B,
                  Function<
                      C,
                      Function<
                          D,
                          Function<
                              E,
                              Function<
                                  F,
                                  Function<
                                      G,
                                      Function<
                                          H,
                                          Function<
                                              I,
                                              Function<
                                                  J,
                                                  Function<K, Function<L, Function<M, Z>>>>>>>>>>>>>
          construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE,
      Field<Z, F> fieldF,
      Field<Z, G> fieldG,
      Field<Z, H> fieldH,
      Field<Z, I> fieldI,
      Field<Z, J> fieldJ,
      Field<Z, K> fieldK,
      Field<Z, L> fieldL,
      Field<Z, M> fieldM) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields =
        new Field[] {
          fieldA, fieldB, fieldC, fieldD, fieldE, fieldF, fieldG, fieldH, fieldI, fieldJ, fieldK,
          fieldL, fieldM
        };
    this.vararg = false;
  }

  /**
   * Creates a 14-field composite codec.
   *
   * @param <A> type of the first field
   * @param <B> type of the second field
   * @param <C> type of the third field
   * @param <D> type of the fourth field
   * @param <E> type of the fifth field
   * @param <F> type of the sixth field
   * @param <G> type of the seventh field
   * @param <H> type of the eighth field
   * @param <I> type of the ninth field
   * @param <J> type of the tenth field
   * @param <K> type of the eleventh field
   * @param <L> type of the twelfth field
   * @param <M> type of the thirteenth field
   * @param <N> type of the fourteenth field
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct curried constructor function
   * @param fieldA first field descriptor
   * @param fieldB second field descriptor
   * @param fieldC third field descriptor
   * @param fieldD fourth field descriptor
   * @param fieldE fifth field descriptor
   * @param fieldF sixth field descriptor
   * @param fieldG seventh field descriptor
   * @param fieldH eighth field descriptor
   * @param fieldI ninth field descriptor
   * @param fieldJ tenth field descriptor
   * @param fieldK eleventh field descriptor
   * @param fieldL twelfth field descriptor
   * @param fieldM thirteenth field descriptor
   * @param fieldN fourteenth field descriptor
   */
  @SuppressWarnings("unchecked")
  public <A, B, C, D, E, F, G, H, I, J, K, L, M, N> CompositeCodec(
      String schema,
      String name,
      Function<
              A,
              Function<
                  B,
                  Function<
                      C,
                      Function<
                          D,
                          Function<
                              E,
                              Function<
                                  F,
                                  Function<
                                      G,
                                      Function<
                                          H,
                                          Function<
                                              I,
                                              Function<
                                                  J,
                                                  Function<
                                                      K,
                                                      Function<
                                                          L, Function<M, Function<N, Z>>>>>>>>>>>>>>
          construct,
      Field<Z, A> fieldA,
      Field<Z, B> fieldB,
      Field<Z, C> fieldC,
      Field<Z, D> fieldD,
      Field<Z, E> fieldE,
      Field<Z, F> fieldF,
      Field<Z, G> fieldG,
      Field<Z, H> fieldH,
      Field<Z, I> fieldI,
      Field<Z, J> fieldJ,
      Field<Z, K> fieldK,
      Field<Z, L> fieldL,
      Field<Z, M> fieldM,
      Field<Z, N> fieldN) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields =
        new Field[] {
          fieldA, fieldB, fieldC, fieldD, fieldE, fieldF, fieldG, fieldH, fieldI, fieldJ, fieldK,
          fieldL, fieldM, fieldN
        };
    this.vararg = false;
  }

  /**
   * Creates a composite codec for any number of fields using an untyped vararg array constructor.
   *
   * <p>This constructor is intended for composite types with more than 14 fields, or whenever the
   * fully-typed curried constructors are impractical. The {@code construct} function receives an
   * {@code Object[]} whose elements correspond positionally to the supplied field descriptors.
   *
   * <p><b>Note:</b> this constructor is less safely typed than the arity-specific overloads.
   * Callers are responsible for casting elements of the array to the correct types.
   *
   * @param schema PostgreSQL schema name, or empty/null for default search path
   * @param name PostgreSQL composite type name
   * @param construct function that maps an {@code Object[]} of decoded field values to {@code Z}
   * @param fields field descriptors in declaration order
   */
  @SafeVarargs
  @SuppressWarnings("unchecked")
  public CompositeCodec(
      String schema, String name, Function<Object[], Z> construct, Field<Z, ?>... fields) {
    this.schema = schema;
    this.pgName = name;
    this.constructor = construct;
    this.fields = fields;
    this.vararg = true;
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
    Object[] values = new Object[fields.length];
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
        values[fieldIdx] = null;
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
        values[fieldIdx] = ((Codec<Object>) fields[fieldIdx].codec).decodeInText(sb, 0).value;
      } else {
        // Unquoted field — pass a subSequence bounded to this field
        int fieldStart = i;
        while (i < len && input.charAt(i) != ',' && input.charAt(i) != ')') {
          i++;
        }
        values[fieldIdx] =
            ((Codec<Object>) fields[fieldIdx].codec)
                .decodeInText(input.subSequence(fieldStart, i), 0)
                .value;
      }
    }
    if (i >= len || input.charAt(i) != ')') {
      throw new Codec.DecodingException(input, i, "Expected ')' to close composite " + pgName);
    }
    return new Codec.ParsingResult<>(applyConstructor(values), i + 1);
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

    Object[] values = new Object[fields.length];
    for (int fieldIdx = 0; fieldIdx < fields.length; fieldIdx++) {
      var field = (Field<Z, Object>) fields[fieldIdx];
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
      if (fieldLen == -1) {
        values[fieldIdx] = null;
      } else {
        values[fieldIdx] = ((Codec<Object>) field.codec).decodeInBinary(buf, fieldLen);
      }
    }
    return applyConstructor(values);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Z random(Random r, int size) {
    Object[] values = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      var field = (Field<Z, Object>) fields[i];
      values[i] = ((Codec<Object>) field.codec).random(r, size);
    }
    return applyConstructor(values);
  }

  @SuppressWarnings("unchecked")
  private Z applyConstructor(Object[] values) {
    if (vararg) {
      return ((Function<Object[], Z>) constructor).apply(values);
    } else {
      Object fn = constructor;
      for (Object v : values) {
        fn = ((Function<Object, Object>) fn).apply(v);
      }
      return (Z) fn;
    }
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
