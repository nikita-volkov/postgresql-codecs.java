package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Codec for a PostgreSQL <a href="https://www.postgresql.org/docs/current/domains.html">domain
 * type</a>. Wraps a base codec and overrides the type identity (schema, name, OIDs) while
 * delegating all encoding and decoding to the base codec.
 *
 * <p>Instances are created via {@link Codec#withType(String, String)} or {@link
 * Codec#withType(String, String, int, int)}.
 *
 * @param <A> the Java value type
 */
final class DomainCodec<A> implements Codec<A> {

  private final Codec<A> base;
  private final String schema;
  private final String name;
  private final int scalarOid;
  private final int arrayOid;

  DomainCodec(Codec<A> base, String schema, String name, int scalarOid, int arrayOid) {
    this.base = base;
    this.schema = schema;
    this.name = name;
    this.scalarOid = scalarOid;
    this.arrayOid = arrayOid;
  }

  @Override
  public String schema() {
    return schema;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public int scalarOid() {
    return scalarOid;
  }

  @Override
  public int arrayOid() {
    return arrayOid;
  }

  @Override
  public void write(StringBuilder sb, A value) {
    base.write(sb, value);
  }

  @Override
  public Codec.ParsingResult<A> parse(CharSequence input, int offset)
      throws Codec.DecodingException {
    return base.parse(input, offset);
  }

  @Override
  public void encodeInBinary(A value, ByteArrayOutputStream out) {
    base.encodeInBinary(value, out);
  }

  @Override
  public A decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    return base.decodeInBinary(buf, length);
  }

  @Override
  public A random(Random r, int size) {
    return base.random(r, size);
  }
}
