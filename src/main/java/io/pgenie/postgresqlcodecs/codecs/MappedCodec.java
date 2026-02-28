package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

final class MappedCodec<A, B> implements Codec<B> {

    private final Codec<A> codec;
    private final Function<A, B> toMapped;
    private final Function<B, A> fromMapped;

    public MappedCodec(Codec<A> codec, Function<A, B> toMapped, Function<B, A> fromMapped) {
        this.codec = codec;
        this.toMapped = toMapped;
        this.fromMapped = fromMapped;
    }

    public String name() {
        return codec.name();
    }

    @Override
    public int oid() {
        return codec.oid();
    }

    @Override
    public int arrayOid() {
        return codec.arrayOid();
    }

    @Override
    public void bind(PreparedStatement ps, int index, B value) throws SQLException {
        codec.bind(ps, index, fromMapped.apply(value));
    }

    public void write(StringBuilder sb, B value) {
        codec.write(sb, fromMapped.apply(value));
    }

    @Override
    public Codec.ParsingResult<B> parse(CharSequence input, int offset) throws Codec.ParseException {
        var result = codec.parse(input, offset);
        return new Codec.ParsingResult<>(toMapped.apply(result.value), result.nextOffset);
    }

    @Override
    public byte[] encode(B value) {
        return codec.encode(fromMapped.apply(value));
    }

    @Override
    public B decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        return toMapped.apply(codec.decodeBinary(buf, length));
    }

}
