package io.codemine.postgresql.codecs;

public class Float8CodecIT extends CodecITBase<Double> {
  public Float8CodecIT() {
    super(Codec.FLOAT8, Double.class);
  }
}
