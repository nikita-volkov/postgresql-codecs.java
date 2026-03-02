package io.pgenie.postgresqlcodecs;

import io.netty.buffer.Unpooled;
import io.pgenie.postgresqlcodecs.codecs.Codec.ParseException;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import java.nio.charset.StandardCharsets;
import reactor.core.publisher.Mono;

/**
 * Adapts a {@link io.pgenie.postgresqlcodecs.codecs.Codec} to the r2dbc-postgresql {@link
 * io.r2dbc.postgresql.codec.Codec} SPI using <b>binary format for parameter encoding</b> and
 * <b>text format for result decoding</b>.
 *
 * <p>Use this with a connection that does <em>not</em> have {@code forceBinary} enabled so that the
 * server returns columns in text format.
 */
public class BinaryInTextOutR2dbcCodec<A> implements io.r2dbc.postgresql.codec.Codec<A> {

  private final io.pgenie.postgresqlcodecs.codecs.Codec<A> codec;
  private final Class<A> type;

  public BinaryInTextOutR2dbcCodec(
      io.pgenie.postgresqlcodecs.codecs.Codec<A> codec, Class<A> type) {
    this.codec = codec;
    this.type = type;
  }

  // -----------------------------------------------------------------------
  // Encoding (binary)
  // -----------------------------------------------------------------------

  @Override
  public boolean canEncode(Object value) {
    return type.isInstance(value);
  }

  @Override
  public boolean canEncodeNull(Class<?> cls) {
    return cls.isAssignableFrom(type);
  }

  @Override
  @SuppressWarnings("unchecked")
  public EncodedParameter encode(Object value) {
    return encode(value, codec.oid());
  }

  @Override
  @SuppressWarnings("unchecked")
  public EncodedParameter encode(Object value, int dataType) {
    byte[] bytes = codec.encodeInBinary((A) value);
    return new EncodedParameter(
        Format.FORMAT_BINARY, dataType, Mono.just(Unpooled.wrappedBuffer(bytes)));
  }

  @Override
  public EncodedParameter encodeNull() {
    return new EncodedParameter(
        Format.FORMAT_BINARY, codec.oid(), Mono.just(Unpooled.EMPTY_BUFFER));
  }

  // -----------------------------------------------------------------------
  // Decoding (text)
  // -----------------------------------------------------------------------

  @Override
  public boolean canDecode(int dataType, Format format, Class<?> requestedType) {
    return format == Format.FORMAT_TEXT && requestedType.isAssignableFrom(type);
  }

  @Override
  public A decode(
      io.netty.buffer.ByteBuf buffer,
      int dataType,
      Format format,
      Class<? extends A> requestedType) {
    if (buffer == null) {
      return null;
    }
    byte[] bytes = new byte[buffer.readableBytes()];
    buffer.readBytes(bytes);
    String text = new String(bytes, StandardCharsets.UTF_8);
    try {
      return codec.parse(text, 0).value;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
}
