package io.codemine.postgresql.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

abstract class CodecTestBase<A> {

  private final Codec<A> codec;
  private final Codec<List<A>> arrayCodec;
  private final Codec<List<List<A>>> arrayArrayCodec;

  @SuppressWarnings("unchecked")
  protected CodecTestBase(Codec<A> codec) {
    this.codec = codec;
    this.arrayCodec = codec.inDim();
    this.arrayArrayCodec = arrayCodec.inDim();
  }

  @Provide
  Arbitrary<A> values() {
    return net.jqwik.api.Arbitraries.fromGeneratorWithSize(
        size -> r -> net.jqwik.api.Shrinkable.unshrinkable(codec.random(r, size)));
  }

  @Provide
  Arbitrary<List<A>> arrayValues() {
    return net.jqwik.api.Arbitraries.fromGeneratorWithSize(
        size -> r -> net.jqwik.api.Shrinkable.unshrinkable(arrayCodec.random(r, size)));
  }

  @Provide
  Arbitrary<List<List<A>>> arrayArrayValues() {
    return net.jqwik.api.Arbitraries.fromGeneratorWithSize(
        size -> r -> net.jqwik.api.Shrinkable.unshrinkable(arrayArrayCodec.random(r, size)));
  }

  @Property(tries = 100)
  void decodesEncodedInBinary(@ForAll("values") A value) throws Exception {
    byte[] encoded = codec.encodeToBytes(value);
    A decoded = codec.decodeInBinary(ByteBuffer.wrap(encoded), encoded.length);
    assertEquals(value, decoded);
  }

  @Property(tries = 100)
  void decodesEncodedInText(@ForAll("values") A value) throws Exception {
    StringBuilder sb = new StringBuilder();
    codec.write(sb, value);
    String encoded = sb.toString();
    A decoded = codec.parse(encoded, 0).value;
    assertEquals(value, decoded);
  }

  @Property(tries = 100)
  void decodesArrayEncodedInBinary(@ForAll("arrayValues") List<A> value) throws Exception {
    byte[] encoded = arrayCodec.encodeToBytes(value);
    List<A> decoded = arrayCodec.decodeInBinary(ByteBuffer.wrap(encoded), encoded.length);
    assertEquals(value, decoded);
  }

  @Property(tries = 100)
  void decodesArrayEncodedInText(@ForAll("arrayValues") List<A> value) throws Exception {
    StringBuilder sb = new StringBuilder();
    arrayCodec.write(sb, value);
    String encoded = sb.toString();
    List<A> decoded = arrayCodec.parse(encoded, 0).value;
    assertEquals(value, decoded);
  }

  @Property(tries = 100)
  void decodesArrayArrayEncodedInBinary(@ForAll("arrayArrayValues") List<List<A>> value)
      throws Exception {
    byte[] encoded = arrayArrayCodec.encodeToBytes(value);
    List<List<A>> decoded =
        arrayArrayCodec.decodeInBinary(ByteBuffer.wrap(encoded), encoded.length);
    assertEquals(value, decoded);
  }

  @Property(tries = 100)
  void decodesArrayArrayEncodedInText(@ForAll("arrayArrayValues") List<List<A>> value)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    arrayArrayCodec.write(sb, value);
    String encoded = sb.toString();
    List<List<A>> decoded = arrayArrayCodec.parse(encoded, 0).value;
    assertEquals(value, decoded);
  }
}
