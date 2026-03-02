package io.pgenie.postgresqlcodecs.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.Function;

final class MappedCodec<A, B> implements Codec<B> {

  private final Codec<A> codec;
  private final Function<A, B> toMapped;
  private final Function<B, A> fromMapped;

  public MappedCodec(Codec<A> codec, Function<A, B> toMapped, Function<B, A> fromMapped) {
    this.codec = codec;
    this.toMapped = toMapped;
    this.fromMapped = fromMapped;
  }

  public String name() {
    return codec.name();
  }

  @Override
  public int scalarOid() {
    return codec.scalarOid();
  }

  @Override
  public int arrayOid() {
    return codec.arrayOid();
  }

  public void write(StringBuilder sb, B value) {
    codec.write(sb, fromMapped.apply(value));
  }

  @Override
  public Codec.ParsingResult<B> parse(CharSequence input, int offset) throws Codec.ParseException {
    var result = codec.parse(input, offset);
    return new Codec.ParsingResult<>(toMapped.apply(result.value), result.nextOffset);
  }

  @Override
  public void encodeInBinary(B value, ByteArrayOutputStream out) {
    codec.encodeInBinary(fromMapped.apply(value), out);
  }

  @Override
  public B decodeInBinary(ByteBuffer buf, int length) throws Codec.ParseException {
    return toMapped.apply(codec.decodeInBinary(buf, length));
  }

  @Override
  public B random(Random r) {
    return toMapped.apply(codec.random(r));
  }
}
