package io.codemine.postgresql.codecs;

public class MoneyCodecIT extends CodecITBase<Long> {
  public MoneyCodecIT() {
    super(Codec.MONEY, Long.class);
  }
}
