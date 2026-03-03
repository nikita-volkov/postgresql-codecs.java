package io.codemine.postgresql.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link EnumCodec}, covering text-format round-trips (binary is not yet
 * implemented).
 */
class EnumCodecTest {

  // -----------------------------------------------------------------------
  // Test enum and codec
  // -----------------------------------------------------------------------
  enum Color {
    RED,
    GREEN,
    BLUE
  }

  private static final EnumCodec<Color> COLOR_CODEC =
      new EnumCodec<>(
          "", "color", Map.of(Color.RED, "red", Color.GREEN, "green", Color.BLUE, "blue"));

  // -----------------------------------------------------------------------
  // Providers
  // -----------------------------------------------------------------------
  @Provide
  Arbitrary<Color> colors() {
    return Arbitraries.randomValue(COLOR_CODEC::random);
  }

  // -----------------------------------------------------------------------
  // Text-format round-trip
  // -----------------------------------------------------------------------
  @Property(tries = 100)
  void decodesEncodedInText(@ForAll("colors") Color value) throws Exception {
    StringBuilder sb = new StringBuilder();
    COLOR_CODEC.write(sb, value);
    String encoded = sb.toString();
    Color decoded = COLOR_CODEC.parse(encoded, 0).value;
    assertEquals(value, decoded, "text round-trip failed for " + value);
  }

  @Test
  void writesCorrectLabels() throws Exception {
    assertEquals("red", writeToString(Color.RED));
    assertEquals("green", writeToString(Color.GREEN));
    assertEquals("blue", writeToString(Color.BLUE));
  }

  @Test
  void parsesCorrectLabels() throws Exception {
    assertEquals(Color.RED, COLOR_CODEC.parse("red", 0).value);
    assertEquals(Color.GREEN, COLOR_CODEC.parse("green", 0).value);
    assertEquals(Color.BLUE, COLOR_CODEC.parse("blue", 0).value);
  }

  @Test
  void parseThrowsOnUnknownLabel() {
    assertThrows(Codec.ParseException.class, () -> COLOR_CODEC.parse("yellow", 0));
  }

  @Test
  void binaryUnsupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> COLOR_CODEC.encodeInBinary(Color.RED, new java.io.ByteArrayOutputStream()));
  }

  @Test
  void typeSig() {
    assertEquals("color", COLOR_CODEC.typeSig());
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------
  private String writeToString(Color value) {
    StringBuilder sb = new StringBuilder();
    COLOR_CODEC.write(sb, value);
    return sb.toString();
  }
}
