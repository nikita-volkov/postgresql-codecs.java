package io.codemine.postgresql.codecs;

public class LineCodecIT extends CodecITBase<Line> {
  public LineCodecIT() {
    super(Codec.LINE, Line.class);
  }
}
