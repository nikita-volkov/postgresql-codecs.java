package io.pgenie.postgresqlcodecs;

import io.netty.buffer.Unpooled;
import io.pgenie.postgresqlcodecs.codecs.Codec.ParseException;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import reactor.core.publisher.Mono;

/**
 * Adapts a {@link io.pgenie.postgresqlcodecs.codecs.Codec} to the r2dbc-postgresql {@link
 * io.r2dbc.postgresql.codec.Codec} SPI so that custom types can be encoded and decoded over the
 * binary extended-query protocol.
 */
public class BinaryInBinaryOutR2dbcCodec<A> implements io.r2dbc.postgresql.codec.Codec<A> {

  private final io.pgenie.postgresqlcodecs.codecs.Codec<A> codec;
  private final Class<A> type;

  public BinaryInBinaryOutR2dbcCodec(
      io.pgenie.postgresqlcodecs.codecs.Codec<A> codec, Class<A> type) {
    this.codec = codec;
    this.type = type;
  }

  // -----------------------------------------------------------------------
  // Encoding
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
    byte[] bytes = codec.encode((A) value);
    return new EncodedParameter(
        Format.FORMAT_BINARY, dataType, Mono.just(Unpooled.wrappedBuffer(bytes)));
  }

  @Override
  public EncodedParameter encodeNull() {
    return new EncodedParameter(
        Format.FORMAT_BINARY, codec.oid(), Mono.just(Unpooled.EMPTY_BUFFER));
  }

  // -----------------------------------------------------------------------
  // Decoding
  // -----------------------------------------------------------------------

  @Override
  public boolean canDecode(int dataType, Format format, Class<?> requestedType) {
    return format == Format.FORMAT_BINARY && requestedType.isAssignableFrom(type);
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
    try {
      return codec.decodeBinary(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN), bytes.length);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
}
