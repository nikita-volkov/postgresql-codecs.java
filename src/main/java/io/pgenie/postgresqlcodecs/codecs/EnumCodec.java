package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public final class EnumCodec<E> implements Codec<E> {

    private final String schema;
    private final String pgName;
    private final Map<E, String> pgLabels;
    private final Map<String, E> byPgLabel;

    public EnumCodec(String schema, String name, Map<E, String> pgLabels) {
        this.schema = schema;
        this.pgName = name;
        this.pgLabels = pgLabels;
        this.byPgLabel = new HashMap<>(pgLabels.size() * 2);
        pgLabels.forEach((constant, label) -> byPgLabel.put(label, constant));
    }

    @Override
    public String schema() {
        return schema;
    }

    @Override
    public String name() {
        return pgName;
    }

    @Override
    public void write(StringBuilder sb, E value) {
        sb.append(pgLabels.get(value));
    }

    @Override
    public Codec.ParsingResult<E> parse(CharSequence input, int offset) throws Codec.ParseException {
        String label = input.subSequence(offset, input.length()).toString();
        E value = byPgLabel.get(label);
        if (value == null) {
            throw new Codec.ParseException(input, offset, "Unknown " + pgName + " value: " + label);
        }
        return new Codec.ParsingResult<>(value, input.length());
    }

    @Override
    public byte[] encode(E value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'encode'");
    }

    @Override
    public E decodeBinary(ByteBuffer buf, int length) throws ParseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'decodeBinary'");
    }

}
