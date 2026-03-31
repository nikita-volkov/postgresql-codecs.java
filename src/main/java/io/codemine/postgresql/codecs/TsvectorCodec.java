package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Codec for PostgreSQL {@code tsvector} values. */
final class TsvectorCodec implements Codec<Tsvector> {

  @Override
  public String name() {
    return "tsvector";
  }

  @Override
  public int scalarOid() {
    return 3614;
  }

  @Override
  public int arrayOid() {
    return 3643;
  }

  // -----------------------------------------------------------------------
  // Textual wire format
  // -----------------------------------------------------------------------
  @Override
  public void encodeInText(StringBuilder sb, Tsvector value) {
    for (int i = 0; i < value.lexemes().size(); i++) {
      if (i > 0) {
        sb.append(' ');
      }
      Tsvector.Lexeme lex = value.lexemes().get(i);
      sb.append('\'');
      sb.append(Tsvector.escapeTsvectorToken(lex.token()));
      sb.append('\'');
      if (!lex.positions().isEmpty()) {
        sb.append(':');
        for (int j = 0; j < lex.positions().size(); j++) {
          if (j > 0) {
            sb.append(',');
          }
          Tsvector.Position p = lex.positions().get(j);
          sb.append(p.pos());
          if (p.weight() != Tsvector.Weight.D) {
            sb.append(p.weight().name());
          }
        }
      }
    }
  }

  @Override
  public Codec.ParsingResult<Tsvector> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    int pos = offset;
    int len = input.length();
    // Skip leading whitespace
    while (pos < len && Character.isWhitespace(input.charAt(pos))) {
      pos++;
    }
    List<Tsvector.Lexeme> lexemes = new ArrayList<>();
    while (pos < len) {
      if (input.charAt(pos) != '\'') {
        break;
      }
      pos++; // skip opening '
      StringBuilder token = new StringBuilder();
      while (pos < len) {
        char c = input.charAt(pos);
        if (c == '\'') {
          if (pos + 1 < len && input.charAt(pos + 1) == '\'') {
            token.append('\'');
            pos += 2;
          } else {
            break;
          }
        } else if (c == '\\') {
          if (pos + 1 < len && input.charAt(pos + 1) == '\\') {
            token.append('\\');
            pos += 2;
          } else {
            token.append(c);
            pos++;
          }
        } else {
          token.append(c);
          pos++;
        }
      }
      if (pos >= len || input.charAt(pos) != '\'') {
        throw new Codec.DecodingException(input, offset, "Unterminated tsvector lexeme");
      }
      pos++; // skip closing '

      List<Tsvector.Position> positions = new ArrayList<>();
      if (pos < len && input.charAt(pos) == ':') {
        pos++; // skip ':'
        while (pos < len) {
          int numStart = pos;
          while (pos < len && Character.isDigit(input.charAt(pos))) {
            pos++;
          }
          if (pos == numStart) {
            break;
          }
          int posNum = Integer.parseInt(input.subSequence(numStart, pos).toString());
          Tsvector.Weight weight = Tsvector.Weight.D;
          if (pos < len) {
            char wc = input.charAt(pos);
            if (wc == 'A') {
              weight = Tsvector.Weight.A;
              pos++;
            } else if (wc == 'B') {
              weight = Tsvector.Weight.B;
              pos++;
            } else if (wc == 'C') {
              weight = Tsvector.Weight.C;
              pos++;
            } else if (wc == 'D') {
              weight = Tsvector.Weight.D;
              pos++;
            }
          }
          positions.add(new Tsvector.Position(posNum, weight));
          if (pos < len && input.charAt(pos) == ',') {
            pos++;
          } else {
            break;
          }
        }
      }
      lexemes.add(new Tsvector.Lexeme(token.toString(), positions));
      // Skip whitespace between lexemes
      while (pos < len && Character.isWhitespace(input.charAt(pos))) {
        pos++;
      }
    }
    Tsvector result = Tsvector.normalize(lexemes);
    return new Codec.ParsingResult<>(result, pos);
  }

  // -----------------------------------------------------------------------
  // Binary wire format
  // -----------------------------------------------------------------------
  @Override
  public void encodeInBinary(Tsvector value, ByteArrayOutputStream out) {
    // 4 bytes: number of lexemes
    writeInt32(out, value.lexemes().size());
    for (Tsvector.Lexeme lex : value.lexemes()) {
      // Null-terminated UTF-8 string
      byte[] tokenBytes = lex.token().getBytes(StandardCharsets.UTF_8);
      out.write(tokenBytes, 0, tokenBytes.length);
      out.write(0); // null terminator
      // 2 bytes: number of positions
      writeInt16(out, lex.positions().size());
      for (Tsvector.Position p : lex.positions()) {
        int weightBits;
        switch (p.weight()) {
          case A:
            weightBits = 3;
            break;
          case B:
            weightBits = 2;
            break;
          case C:
            weightBits = 1;
            break;
          default:
            weightBits = 0;
            break;
        }
        int posVal = Math.max(1, Math.min(16383, p.pos()));
        writeInt16(out, (weightBits << 14) | posVal);
      }
    }
  }

  @Override
  public Tsvector decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    int numLexemes = buf.getInt();
    if (numLexemes < 0) {
      throw new Codec.DecodingException("Negative lexeme count in tsvector binary data");
    }
    List<Tsvector.Lexeme> lexemes = new ArrayList<>(numLexemes);
    for (int i = 0; i < numLexemes; i++) {
      // Read null-terminated UTF-8 string
      ByteArrayOutputStream tokenBuf = new ByteArrayOutputStream();
      while (buf.hasRemaining()) {
        byte b = buf.get();
        if (b == 0) {
          break;
        }
        tokenBuf.write(b);
      }
      String token = tokenBuf.toString(StandardCharsets.UTF_8);

      int numPositions = buf.getShort() & 0xFFFF;
      List<Tsvector.Position> positions = new ArrayList<>(numPositions);
      for (int j = 0; j < numPositions; j++) {
        int posWord = buf.getShort() & 0xFFFF;
        int weightBits = (posWord >>> 14) & 0x3;
        Tsvector.Weight weight;
        switch (weightBits) {
          case 3:
            weight = Tsvector.Weight.A;
            break;
          case 2:
            weight = Tsvector.Weight.B;
            break;
          case 1:
            weight = Tsvector.Weight.C;
            break;
          default:
            weight = Tsvector.Weight.D;
            break;
        }
        int posNum = posWord & 0x3FFF;
        positions.add(new Tsvector.Position(posNum, weight));
      }
      lexemes.add(new Tsvector.Lexeme(token, positions));
    }
    return new Tsvector(lexemes);
  }

  // -----------------------------------------------------------------------
  // Random generation
  // -----------------------------------------------------------------------
  @Override
  public Tsvector random(Random r, int size) {
    int numLexemes = size == 0 ? 0 : r.nextInt(Math.min(size, 10) + 1);
    List<Tsvector.Lexeme> lexemes = new ArrayList<>(numLexemes);
    for (int i = 0; i < numLexemes; i++) {
      int tokenLen = r.nextInt(10) + 1;
      StringBuilder token = new StringBuilder(tokenLen);
      for (int j = 0; j < tokenLen; j++) {
        token.append((char) ('a' + r.nextInt(26)));
      }
      int numPositions = r.nextInt(4);
      List<Tsvector.Position> positions = new ArrayList<>(numPositions);
      for (int j = 0; j < numPositions; j++) {
        int posNum = r.nextInt(16383) + 1;
        Tsvector.Weight weight = Tsvector.Weight.values()[r.nextInt(4)];
        positions.add(new Tsvector.Position(posNum, weight));
      }
      lexemes.add(new Tsvector.Lexeme(token.toString(), positions));
    }
    return Tsvector.normalize(lexemes);
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------
  private static void writeInt32(ByteArrayOutputStream out, int v) {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 8) & 0xFF);
    out.write(v & 0xFF);
  }

  private static void writeInt16(ByteArrayOutputStream out, int v) {
    out.write((v >>> 8) & 0xFF);
    out.write(v & 0xFF);
  }
}
