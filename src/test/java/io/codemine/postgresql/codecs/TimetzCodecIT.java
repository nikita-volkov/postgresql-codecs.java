package io.codemine.postgresql.codecs;

public class TimetzCodecIT extends CodecITBase<Timetz> {
  public TimetzCodecIT() {
    super(Codec.TIMETZ, Timetz.class);
  }
}
