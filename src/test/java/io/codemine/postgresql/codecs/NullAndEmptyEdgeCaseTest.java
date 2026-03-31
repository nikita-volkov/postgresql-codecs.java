package io.codemine.postgresql.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Explicit edge-case tests for null values and empty strings in scalars, arrays, and composite
 * types — both text and binary wire formats.
 *
 * <p>The property-based tests in {@link CodecTestBase} never generate null array elements or null
 * composite fields because the {@code random()} methods don't produce nulls. These tests fill that
 * gap by exercising the null / empty-string paths explicitly.
 */
class NullAndEmptyEdgeCaseTest {

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /** Encodes {@code value} as text, then decodes it and asserts round-trip equality. */
  private static <A> void assertTextRoundTrip(Codec<A> codec, A value) throws Exception {
    StringBuilder sb = new StringBuilder();
    codec.encodeInText(sb, value);
    String encoded = sb.toString();
    A decoded = codec.decodeInText(encoded, 0).value;
    assertEquals(value, decoded);
  }

  /** Encodes {@code value} in binary, then decodes it and asserts round-trip equality. */
  private static <A> void assertBinaryRoundTrip(Codec<A> codec, A value) throws Exception {
    byte[] encoded = codec.encodeInBinaryToBytes(value);
    A decoded = codec.decodeInBinary(ByteBuffer.wrap(encoded), encoded.length);
    assertEquals(value, decoded);
  }

  // -----------------------------------------------------------------------
  // Empty string — scalar text codec
  // -----------------------------------------------------------------------

  @Test
  void emptyString_textRoundTrip() throws Exception {
    assertTextRoundTrip(Codec.TEXT, "");
  }

  @Test
  void emptyString_binaryRoundTrip() throws Exception {
    assertBinaryRoundTrip(Codec.TEXT, "");
  }

  // -----------------------------------------------------------------------
  // Empty array
  // -----------------------------------------------------------------------

  @Nested
  class EmptyArray {

    @Test
    void textArray_emptyArray_textRoundTrip() throws Exception {
      Codec<List<String>> arrayCodec = Codec.TEXT.inDim();
      List<String> value = List.of();
      StringBuilder sb = new StringBuilder();
      arrayCodec.encodeInText(sb, value);
      assertEquals("{}", sb.toString());
      assertTextRoundTrip(arrayCodec, value);
    }

    @Test
    void textArray_emptyArray_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), List.of());
    }

    @Test
    void int4Array_emptyArray_textRoundTrip() throws Exception {
      assertTextRoundTrip(Codec.INT4.inDim(), List.of());
    }

    @Test
    void int4Array_emptyArray_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.INT4.inDim(), List.of());
    }

    @Test
    void textArray2D_emptyOuterArray_textRoundTrip() throws Exception {
      assertTextRoundTrip(Codec.TEXT.inDim().inDim(), List.of());
    }

    @Test
    void textArray2D_emptyOuterArray_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim().inDim(), List.of());
    }

    @Test
    void textArray2D_outerArrayWithOneEmptyInner_textRoundTrip() throws Exception {
      assertTextRoundTrip(Codec.TEXT.inDim().inDim(), List.of(List.of()));
    }

    @Test
    void textArray2D_outerArrayWithOneEmptyInner_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim().inDim(), List.of(List.of()));
    }
  }

  // -----------------------------------------------------------------------
  // Null elements in arrays
  // -----------------------------------------------------------------------

  @Nested
  class NullElementsInArrays {

    @Test
    void singleNull_textRoundTrip() throws Exception {
      List<String> value = Arrays.asList((String) null);
      Codec<List<String>> arrayCodec = Codec.TEXT.inDim();
      StringBuilder sb = new StringBuilder();
      arrayCodec.encodeInText(sb, value);
      assertEquals("{NULL}", sb.toString());
      assertTextRoundTrip(arrayCodec, value);
    }

    @Test
    void singleNull_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), Arrays.asList((String) null));
    }

    @Test
    void nullInMiddle_textRoundTrip() throws Exception {
      List<String> value = Arrays.asList("a", null, "b");
      Codec<List<String>> arrayCodec = Codec.TEXT.inDim();
      StringBuilder sb = new StringBuilder();
      arrayCodec.encodeInText(sb, value);
      assertEquals("{a,NULL,b}", sb.toString());
      assertTextRoundTrip(arrayCodec, value);
    }

    @Test
    void nullInMiddle_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), Arrays.asList("a", null, "b"));
    }

    @Test
    void nullAtStart_textRoundTrip() throws Exception {
      assertTextRoundTrip(Codec.TEXT.inDim(), Arrays.asList(null, "a", "b"));
    }

    @Test
    void nullAtStart_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), Arrays.asList(null, "a", "b"));
    }

    @Test
    void nullAtEnd_textRoundTrip() throws Exception {
      assertTextRoundTrip(Codec.TEXT.inDim(), Arrays.asList("a", "b", null));
    }

    @Test
    void nullAtEnd_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), Arrays.asList("a", "b", null));
    }

    @Test
    void allNulls_textRoundTrip() throws Exception {
      List<String> value = Arrays.asList(null, null, null);
      Codec<List<String>> arrayCodec = Codec.TEXT.inDim();
      StringBuilder sb = new StringBuilder();
      arrayCodec.encodeInText(sb, value);
      assertEquals("{NULL,NULL,NULL}", sb.toString());
      assertTextRoundTrip(arrayCodec, value);
    }

    @Test
    void allNulls_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), Arrays.asList(null, null, null));
    }

    @Test
    void int4ArrayWithNulls_textRoundTrip() throws Exception {
      assertTextRoundTrip(Codec.INT4.inDim(), Arrays.asList(1, null, 3));
    }

    @Test
    void int4ArrayWithNulls_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.INT4.inDim(), Arrays.asList(1, null, 3));
    }
  }

  // -----------------------------------------------------------------------
  // Empty strings in arrays (distinct from null)
  // -----------------------------------------------------------------------

  @Nested
  class EmptyStringsInArrays {

    @Test
    void singleEmptyString_textRoundTrip() throws Exception {
      // An empty-string element must be encoded as "" (double-quoted) to distinguish from null.
      List<String> value = List.of("");
      Codec<List<String>> arrayCodec = Codec.TEXT.inDim();
      StringBuilder sb = new StringBuilder();
      arrayCodec.encodeInText(sb, value);
      assertEquals("{\"\"}", sb.toString());
      assertTextRoundTrip(arrayCodec, value);
    }

    @Test
    void singleEmptyString_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), List.of(""));
    }

    @Test
    void emptyStringInMiddle_textRoundTrip() throws Exception {
      List<String> value = List.of("a", "", "b");
      Codec<List<String>> arrayCodec = Codec.TEXT.inDim();
      StringBuilder sb = new StringBuilder();
      arrayCodec.encodeInText(sb, value);
      assertEquals("{a,\"\",b}", sb.toString());
      assertTextRoundTrip(arrayCodec, value);
    }

    @Test
    void emptyStringInMiddle_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), List.of("a", "", "b"));
    }

    @Test
    void allEmptyStrings_textRoundTrip() throws Exception {
      assertTextRoundTrip(Codec.TEXT.inDim(), List.of("", "", ""));
    }

    @Test
    void allEmptyStrings_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), List.of("", "", ""));
    }

    /** The string literal {@code "NULL"} must be quoted so it round-trips as a string, not null. */
    @Test
    void nullLiteralString_textRoundTrip() throws Exception {
      List<String> value = List.of("NULL");
      Codec<List<String>> arrayCodec = Codec.TEXT.inDim();
      StringBuilder sb = new StringBuilder();
      arrayCodec.encodeInText(sb, value);
      assertEquals("{\"NULL\"}", sb.toString());
      assertTextRoundTrip(arrayCodec, value);
    }

    @Test
    void nullLiteralString_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), List.of("NULL"));
    }

    /** Case-insensitive NULL variants should also be quoted. */
    @Test
    void nullLiteralStringLowercase_textRoundTrip() throws Exception {
      List<String> value = List.of("null");
      Codec<List<String>> arrayCodec = Codec.TEXT.inDim();
      StringBuilder sb = new StringBuilder();
      arrayCodec.encodeInText(sb, value);
      assertEquals("{\"null\"}", sb.toString());
      assertTextRoundTrip(arrayCodec, value);
    }

    @Test
    void nullLiteralStringMixedCase_textRoundTrip() throws Exception {
      List<String> value = List.of("Null");
      Codec<List<String>> arrayCodec = Codec.TEXT.inDim();
      StringBuilder sb = new StringBuilder();
      arrayCodec.encodeInText(sb, value);
      assertEquals("{\"Null\"}", sb.toString());
      assertTextRoundTrip(arrayCodec, value);
    }

    @Test
    void mixedNullAndEmptyString_textRoundTrip() throws Exception {
      assertTextRoundTrip(Codec.TEXT.inDim(), Arrays.asList(null, "", null, "a"));
    }

    @Test
    void mixedNullAndEmptyString_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(Codec.TEXT.inDim(), Arrays.asList(null, "", null, "a"));
    }
  }

  // -----------------------------------------------------------------------
  // Null fields in composite types
  // -----------------------------------------------------------------------

  record NullableTextPair(String first, String second) {}

  static final CompositeCodec<NullableTextPair> NULLABLE_TEXT_PAIR_CODEC =
      new CompositeCodec<>(
          "",
          "nullable_text_pair",
          args -> new NullableTextPair((String) args[0], (String) args[1]),
          new CompositeCodec.Field<>("first", NullableTextPair::first, Codec.TEXT),
          new CompositeCodec.Field<>("second", NullableTextPair::second, Codec.TEXT));

  @Nested
  class NullFieldsInComposites {

    @Test
    void nullFirstField_textEncoding() throws Exception {
      NullableTextPair value = new NullableTextPair(null, "hello");
      StringBuilder sb = new StringBuilder();
      NULLABLE_TEXT_PAIR_CODEC.encodeInText(sb, value);
      assertEquals("(,hello)", sb.toString());
    }

    @Test
    void nullFirstField_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair(null, "hello"));
    }

    @Test
    void nullFirstField_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair(null, "hello"));
    }

    @Test
    void nullLastField_textEncoding() throws Exception {
      NullableTextPair value = new NullableTextPair("hello", null);
      StringBuilder sb = new StringBuilder();
      NULLABLE_TEXT_PAIR_CODEC.encodeInText(sb, value);
      assertEquals("(hello,)", sb.toString());
    }

    @Test
    void nullLastField_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair("hello", null));
    }

    @Test
    void nullLastField_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair("hello", null));
    }

    @Test
    void allNullFields_textEncoding() throws Exception {
      NullableTextPair value = new NullableTextPair(null, null);
      StringBuilder sb = new StringBuilder();
      NULLABLE_TEXT_PAIR_CODEC.encodeInText(sb, value);
      assertEquals("(,)", sb.toString());
    }

    @Test
    void allNullFields_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair(null, null));
    }

    @Test
    void allNullFields_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair(null, null));
    }
  }

  // -----------------------------------------------------------------------
  // Empty string fields in composite types (distinct from null)
  // -----------------------------------------------------------------------

  @Nested
  class EmptyStringFieldsInComposites {

    @Test
    void emptyStringFirstField_textEncoding() throws Exception {
      NullableTextPair value = new NullableTextPair("", "hello");
      StringBuilder sb = new StringBuilder();
      NULLABLE_TEXT_PAIR_CODEC.encodeInText(sb, value);
      // An empty text field must be encoded as "" to distinguish it from NULL.
      assertEquals("(\"\",hello)", sb.toString());
    }

    @Test
    void emptyStringFirstField_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair("", "hello"));
    }

    @Test
    void emptyStringFirstField_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair("", "hello"));
    }

    @Test
    void emptyStringLastField_textEncoding() throws Exception {
      NullableTextPair value = new NullableTextPair("hello", "");
      StringBuilder sb = new StringBuilder();
      NULLABLE_TEXT_PAIR_CODEC.encodeInText(sb, value);
      assertEquals("(hello,\"\")", sb.toString());
    }

    @Test
    void emptyStringLastField_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair("hello", ""));
    }

    @Test
    void emptyStringLastField_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair("hello", ""));
    }

    @Test
    void allEmptyStringFields_textEncoding() throws Exception {
      NullableTextPair value = new NullableTextPair("", "");
      StringBuilder sb = new StringBuilder();
      NULLABLE_TEXT_PAIR_CODEC.encodeInText(sb, value);
      assertEquals("(\"\",\"\")", sb.toString());
    }

    @Test
    void allEmptyStringFields_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair("", ""));
    }

    @Test
    void allEmptyStringFields_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TEXT_PAIR_CODEC, new NullableTextPair("", ""));
    }
  }

  // -----------------------------------------------------------------------
  // Null / empty array fields inside composite types
  // -----------------------------------------------------------------------

  record NullableTaggedData(String tag, List<String> items) {}

  @SuppressWarnings("unchecked")
  static final CompositeCodec<NullableTaggedData> NULLABLE_TAGGED_CODEC =
      new CompositeCodec<>(
          "",
          "nullable_tagged",
          args -> new NullableTaggedData((String) args[0], (List<String>) args[1]),
          new CompositeCodec.Field<>("tag", NullableTaggedData::tag, Codec.TEXT),
          new CompositeCodec.Field<>("items", NullableTaggedData::items, Codec.TEXT.inDim()));

  @Nested
  class ArrayFieldsInComposites {

    @Test
    void nullArrayField_textEncoding() throws Exception {
      NullableTaggedData value = new NullableTaggedData("hello", null);
      StringBuilder sb = new StringBuilder();
      NULLABLE_TAGGED_CODEC.encodeInText(sb, value);
      assertEquals("(hello,)", sb.toString());
    }

    @Test
    void nullArrayField_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TAGGED_CODEC, new NullableTaggedData("hello", null));
    }

    @Test
    void nullArrayField_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TAGGED_CODEC, new NullableTaggedData("hello", null));
    }

    @Test
    void emptyArrayField_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TAGGED_CODEC, new NullableTaggedData("hello", List.of()));
    }

    @Test
    void emptyArrayField_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TAGGED_CODEC, new NullableTaggedData("hello", List.of()));
    }

    @Test
    void arrayFieldWithNullElements_textRoundTrip() throws Exception {
      assertTextRoundTrip(
          NULLABLE_TAGGED_CODEC, new NullableTaggedData("hello", Arrays.asList("a", null, "b")));
    }

    @Test
    void arrayFieldWithNullElements_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(
          NULLABLE_TAGGED_CODEC, new NullableTaggedData("hello", Arrays.asList("a", null, "b")));
    }

    @Test
    void arrayFieldWithEmptyStrings_textRoundTrip() throws Exception {
      assertTextRoundTrip(
          NULLABLE_TAGGED_CODEC, new NullableTaggedData("hello", List.of("", "x", "")));
    }

    @Test
    void arrayFieldWithEmptyStrings_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(
          NULLABLE_TAGGED_CODEC, new NullableTaggedData("hello", List.of("", "x", "")));
    }

    @Test
    void allNullFields_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TAGGED_CODEC, new NullableTaggedData(null, null));
    }

    @Test
    void allNullFields_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TAGGED_CODEC, new NullableTaggedData(null, null));
    }

    @Test
    void nullTagWithEmptyArray_textRoundTrip() throws Exception {
      assertTextRoundTrip(NULLABLE_TAGGED_CODEC, new NullableTaggedData(null, List.of()));
    }

    @Test
    void nullTagWithEmptyArray_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(NULLABLE_TAGGED_CODEC, new NullableTaggedData(null, List.of()));
    }
  }

  // -----------------------------------------------------------------------
  // 3-field composite with null in the middle
  // -----------------------------------------------------------------------

  record Triple(String a, String b, String c) {}

  static final CompositeCodec<Triple> TRIPLE_CODEC =
      new CompositeCodec<>(
          "",
          "triple",
          args -> new Triple((String) args[0], (String) args[1], (String) args[2]),
          new CompositeCodec.Field<>("a", Triple::a, Codec.TEXT),
          new CompositeCodec.Field<>("b", Triple::b, Codec.TEXT),
          new CompositeCodec.Field<>("c", Triple::c, Codec.TEXT));

  @Nested
  class ThreeFieldCompositeNulls {

    @Test
    void nullMiddleField_textEncoding() throws Exception {
      Triple value = new Triple("first", null, "third");
      StringBuilder sb = new StringBuilder();
      TRIPLE_CODEC.encodeInText(sb, value);
      assertEquals("(first,,third)", sb.toString());
    }

    @Test
    void nullMiddleField_textRoundTrip() throws Exception {
      assertTextRoundTrip(TRIPLE_CODEC, new Triple("first", null, "third"));
    }

    @Test
    void nullMiddleField_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(TRIPLE_CODEC, new Triple("first", null, "third"));
    }

    @Test
    void allNulls_textEncoding() throws Exception {
      Triple value = new Triple(null, null, null);
      StringBuilder sb = new StringBuilder();
      TRIPLE_CODEC.encodeInText(sb, value);
      assertEquals("(,,)", sb.toString());
    }

    @Test
    void allNulls_textRoundTrip() throws Exception {
      assertTextRoundTrip(TRIPLE_CODEC, new Triple(null, null, null));
    }

    @Test
    void allNulls_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(TRIPLE_CODEC, new Triple(null, null, null));
    }

    @Test
    void mixedNullsAndEmptyStrings_textRoundTrip() throws Exception {
      assertTextRoundTrip(TRIPLE_CODEC, new Triple(null, "", null));
    }

    @Test
    void mixedNullsAndEmptyStrings_binaryRoundTrip() throws Exception {
      assertBinaryRoundTrip(TRIPLE_CODEC, new Triple(null, "", null));
    }
  }
}
