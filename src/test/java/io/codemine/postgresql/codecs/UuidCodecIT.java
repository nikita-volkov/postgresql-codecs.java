package io.codemine.postgresql.codecs;

import java.util.UUID;

public class UuidCodecIT extends CodecITBase<UUID> {
  public UuidCodecIT() {
    super(Codec.UUID, UUID.class);
  }
}
