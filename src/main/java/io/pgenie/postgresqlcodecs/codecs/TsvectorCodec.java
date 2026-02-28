package io.pgenie.postgresqlcodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.postgresql.util.PGobject;

import io.pgenie.postgresqlcodecs.types.Tsvector;

final class TsvectorCodec implements Codec<Tsvector> {

    static final TsvectorCodec instance = new TsvectorCodec();

    private TsvectorCodec() {
    }

    public String name() {
        return "tsvector";
    }

    @Override
    public int oid() {
        return 3614;
    }

    @Override
    public int arrayOid() {
        return 3643;
    }

    @Override
    public void bind(PreparedStatement ps, int index, Tsvector value) throws SQLException {
        if (value != null) {
            PGobject obj = new PGobject();
            obj.setType("tsvector");
            obj.setValue(tsvectorToText(value));
            ps.setObject(index, obj);
        } else {
            ps.setNull(index, Types.OTHER);
        }
    }

    @Override
    public void write(StringBuilder sb, Tsvector value) {
        sb.append(tsvectorToText(value));
    }

    @Override
    public Codec.ParsingResult<Tsvector> parse(CharSequence input, int offset) throws Codec.ParseException {
        String s = input.subSequence(offset, input.length()).toString();
        try {
            return new Codec.ParsingResult<>(parseTsvector(s), input.length());
        } catch (Exception e) {
            throw new Codec.ParseException(input, offset, "Invalid tsvector: " + e.getMessage());
        }
    }

    @Override
    public byte[] encode(Tsvector value) {
        List<Map.Entry<String, List<Tsvector.Position>>> lexemes = value.toLexemeList();
        // First pass: calculate total size
        int size = 4; // numLexemes int32
        for (var entry : lexemes) {
            byte[] tokenBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            size += tokenBytes.length + 1; // null-terminated
            size += 2; // numPositions uint16
            size += entry.getValue().size() * 2; // each position is uint16
        }
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(lexemes.size());
        for (var entry : lexemes) {
            byte[] tokenBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            buf.put(tokenBytes);
            buf.put((byte) 0); // null terminator
            List<Tsvector.Position> positions = entry.getValue();
            buf.putShort((short) positions.size());
            for (var pos : positions) {
                // bits 14-15 = weight (A=3, B=2, C=1, D=0), bits 0-13 = position
                int posWord = (pos.weight().toBits() << 14) | (pos.pos() & 0x3FFF);
                buf.putShort((short) posWord);
            }
        }
        return buf.array();
    }

    @Override
    public Tsvector decodeBinary(ByteBuffer buf, int length) throws Codec.ParseException {
        int numLexemes = buf.getInt();
        List<Map.Entry<String, List<Tsvector.Position>>> entries = new ArrayList<>(numLexemes);
        for (int i = 0; i < numLexemes; i++) {
            // Read null-terminated UTF-8 token
            int start = buf.position();
            while (buf.get() != 0) { /* scan to null */ }
            int end = buf.position() - 1;
            byte[] tokenBytes = new byte[end - start];
            int savedPos = buf.position();
            buf.position(start);
            buf.get(tokenBytes);
            buf.position(savedPos); // skip past the null
            String token = new String(tokenBytes, StandardCharsets.UTF_8);

            int numPositions = Short.toUnsignedInt(buf.getShort());
            List<Tsvector.Position> positions = new ArrayList<>(numPositions);
            for (int j = 0; j < numPositions; j++) {
                int posWord = Short.toUnsignedInt(buf.getShort());
                int weightBits = (posWord >>> 14) & 0x3;
                short posVal = (short) (posWord & 0x3FFF);
                positions.add(new Tsvector.Position(posVal, Tsvector.Weight.fromBits(weightBits)));
            }
            entries.add(Map.entry(token, positions));
        }
        return Tsvector.of(entries);
    }

    // -----------------------------------------------------------------------
    // Text format helpers
    // -----------------------------------------------------------------------

    /**
     * Serialises a {@link Tsvector} to PostgreSQL text format:
     * {@code 'lexeme1':1A,2 'lexeme2':3B}
     */
    static String tsvectorToText(Tsvector ts) {
        StringBuilder sb = new StringBuilder();
        for (var entry : ts.toLexemeList()) {
            if (sb.length() > 0) sb.append(' ');
            sb.append('\'');
            escapeToken(sb, entry.getKey());
            sb.append('\'');
            List<Tsvector.Position> positions = entry.getValue();
            if (!positions.isEmpty()) {
                sb.append(':');
                for (int i = 0; i < positions.size(); i++) {
                    if (i > 0) sb.append(',');
                    var p = positions.get(i);
                    sb.append(Short.toUnsignedInt(p.pos()));
                    if (p.weight() != Tsvector.Weight.D) sb.append(p.weight().name());
                }
            }
        }
        return sb.toString();
    }

    private static void escapeToken(StringBuilder sb, String token) {
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if (c == '\'') sb.append("''");
            else if (c == '\\') sb.append("\\\\");
            else sb.append(c);
        }
    }

    /**
     * Parses a PostgreSQL tsvector text representation.
     */
    static Tsvector parseTsvector(String s) throws Exception {
        List<Map.Entry<String, List<Tsvector.Position>>> entries = new ArrayList<>();
        int i = 0;
        int len = s.length();
        while (i < len) {
            // skip whitespace
            while (i < len && Character.isWhitespace(s.charAt(i))) i++;
            if (i >= len) break;
            if (s.charAt(i) != '\'') throw new Exception("Expected ' at position " + i);
            i++; // skip opening '
            // Parse quoted token
            StringBuilder token = new StringBuilder();
            while (i < len) {
                char c = s.charAt(i);
                if (c == '\'') {
                    if (i + 1 < len && s.charAt(i + 1) == '\'') {
                        token.append('\'');
                        i += 2;
                    } else {
                        i++; // skip closing '
                        break;
                    }
                } else if (c == '\\' && i + 1 < len && s.charAt(i + 1) == '\\') {
                    token.append('\\');
                    i += 2;
                } else {
                    token.append(c);
                    i++;
                }
            }
            // Parse optional positions: :1A,2B,...
            List<Tsvector.Position> positions = new ArrayList<>();
            if (i < len && s.charAt(i) == ':') {
                i++; // skip ':'
                while (i < len && !Character.isWhitespace(s.charAt(i))) {
                    // parse position number
                    int start2 = i;
                    while (i < len && Character.isDigit(s.charAt(i))) i++;
                    int pos = Integer.parseInt(s.substring(start2, i));
                    // parse optional weight letter
                    Tsvector.Weight weight = Tsvector.Weight.D;
                    if (i < len) {
                        char wc = s.charAt(i);
                        if (wc == 'A' || wc == 'a') { weight = Tsvector.Weight.A; i++; }
                        else if (wc == 'B' || wc == 'b') { weight = Tsvector.Weight.B; i++; }
                        else if (wc == 'C' || wc == 'c') { weight = Tsvector.Weight.C; i++; }
                        else if (wc == 'D' || wc == 'd') { weight = Tsvector.Weight.D; i++; }
                    }
                    positions.add(new Tsvector.Position((short) pos, weight));
                    if (i < len && s.charAt(i) == ',') i++;
                }
            }
            entries.add(Map.entry(token.toString(), positions));
        }
        return Tsvector.of(entries);
    }

}
