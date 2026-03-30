package io.codemine.postgresql.codecs;

public class PolygonCodecIT extends CodecITBase<Polygon> {
  public PolygonCodecIT() {
    super(Codec.POLYGON, Polygon.class);
  }
}
