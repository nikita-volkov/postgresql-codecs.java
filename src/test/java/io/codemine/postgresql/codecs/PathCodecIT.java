package io.codemine.postgresql.codecs;

public class PathCodecIT extends CodecITBase<PgPath> {
  public PathCodecIT() {
    super(Codec.PATH, PgPath.class);
  }
}
