package io.codemine.postgresql.codecs;

public class PointCodecIT extends CodecITBase<Point> {
  public PointCodecIT() {
    super(Codec.POINT, Point.class);
  }
}
