package io.codemine.postgresql.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Codec#withType} (domain type support).
 *
 * <p>Verifies that the returned codec overrides type metadata (schema, name, OIDs) while delegating
 * all encoding and decoding unchanged to the base codec.
 */
class DomainCodecTest {

  // A domain based on int4: CREATE DOMAIN positive_int AS int4 CHECK (VALUE > 0)
  private static final Codec<Integer> POSITIVE_INT =
      Codec.INT4.withType("public", "positive_int", 99001, 99002);

  // A domain with unknown OIDs (withType without explicit OIDs)
  private static final Codec<String> EMAIL = Codec.TEXT.withType("", "email");

  // -----------------------------------------------------------------------
  // Metadata delegation
  // -----------------------------------------------------------------------

  @Test
  void nameIsOverridden() {
    assertEquals("positive_int", POSITIVE_INT.name());
  }

  @Test
  void schemaIsOverridden() {
    assertEquals("public", POSITIVE_INT.schema());
  }

  @Test
  void scalarOidIsOverridden() {
    assertEquals(99001, POSITIVE_INT.scalarOid());
  }

  @Test
  void arrayOidIsOverridden() {
    assertEquals(99002, POSITIVE_INT.arrayOid());
  }

  @Test
  void typeSigIncludesSchemaAndName() {
    assertEquals("public.positive_int", POSITIVE_INT.typeSig());
  }

  @Test
  void withTypeWithoutOidsDefaultsToZero() {
    assertEquals("email", EMAIL.name());
    assertEquals("", EMAIL.schema());
    assertEquals(0, EMAIL.scalarOid());
    assertEquals(0, EMAIL.arrayOid());
  }

  // -----------------------------------------------------------------------
  // Encoding / decoding delegation (text)
  // -----------------------------------------------------------------------

  @Test
  void encodeAndDecodeInTextDelegatesToBase() throws Exception {
    StringBuilder sb = new StringBuilder();
    POSITIVE_INT.encodeInText(sb, 42);
    assertEquals("42", sb.toString());

    Codec.ParsingResult<Integer> result = POSITIVE_INT.decodeInText("42", 0);
    assertEquals(42, result.value);
  }

  @Test
  void encodeInTextToStringDelegatesToBase() {
    assertEquals("42", POSITIVE_INT.encodeInTextToString(42));
  }

  @Test
  void decodeInTextFromStringDelegatesToBase() throws Exception {
    assertEquals(42, POSITIVE_INT.decodeInTextFromString("42"));
  }

  // -----------------------------------------------------------------------
  // Encoding / decoding delegation (binary)
  // -----------------------------------------------------------------------

  @Test
  void encodeAndDecodeInBinaryDelegatesToBase() throws Exception {
    byte[] bytes = POSITIVE_INT.encodeInBinaryToBytes(42);
    Integer decoded = POSITIVE_INT.decodeInBinary(ByteBuffer.wrap(bytes), bytes.length);
    assertEquals(42, decoded);
  }

  @Test
  void decodeInBinaryFromBytesDelegatesToBase() throws Exception {
    byte[] bytes = POSITIVE_INT.encodeInBinaryToBytes(42);
    assertEquals(42, POSITIVE_INT.decodeInBinaryFromBytes(bytes));
  }

  // -----------------------------------------------------------------------
  // Property: domain codec round-trips match base codec
  // -----------------------------------------------------------------------

  @Provide
  Arbitrary<Integer> int4Values() {
    return net.jqwik.api.Arbitraries.fromGeneratorWithSize(
        size -> r -> net.jqwik.api.Shrinkable.unshrinkable(Codec.INT4.random(r, size)));
  }

  @Property(tries = 100)
  void textRoundtripMatchesBaseCodec(@ForAll("int4Values") Integer value) throws Exception {
    String baseEncoded = Codec.INT4.encodeInTextToString(value);
    String domainEncoded = POSITIVE_INT.encodeInTextToString(value);
    assertEquals(baseEncoded, domainEncoded);

    Integer baseDec = Codec.INT4.decodeInTextFromString(baseEncoded);
    Integer domainDec = POSITIVE_INT.decodeInTextFromString(domainEncoded);
    assertEquals(baseDec, domainDec);
  }

  @Property(tries = 100)
  void binaryRoundtripMatchesBaseCodec(@ForAll("int4Values") Integer value) throws Exception {
    byte[] baseBytes = Codec.INT4.encodeInBinaryToBytes(value);
    byte[] domainBytes = POSITIVE_INT.encodeInBinaryToBytes(value);
    assertEquals(baseBytes.length, domainBytes.length);

    Integer baseDec = Codec.INT4.decodeInBinaryFromBytes(baseBytes);
    Integer domainDec = POSITIVE_INT.decodeInBinaryFromBytes(domainBytes);
    assertEquals(baseDec, domainDec);
  }
}
