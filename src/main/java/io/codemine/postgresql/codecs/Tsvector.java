package io.codemine.postgresql.codecs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * PostgreSQL {@code tsvector} type. A sorted list of distinct lexemes, each optionally annotated
 * with position and weight information.
 *
 * <p>Lexemes are sorted alphabetically and deduplicated, matching PostgreSQL's canonical
 * representation. Each lexeme has zero or more (position, weight) pairs, where positions range from
 * 1 to 16383.
 *
 * @param lexemes sorted list of (lexeme, positions) entries in canonical form
 */
public record Tsvector(List<Lexeme> lexemes) {

  /** A single lexeme entry with its token text and associated positions. */
  public record Lexeme(String token, List<Position> positions) {

    /** Compact constructor that validates and makes an immutable copy of positions. */
    public Lexeme {
      Objects.requireNonNull(token);
      positions = List.copyOf(positions);
    }
  }

  /** A position-weight pair within a lexeme. Position is 1..16383. */
  public record Position(int pos, Weight weight) {

    /** Compact constructor that validates the weight is non-null. */
    public Position {
      Objects.requireNonNull(weight);
    }
  }

  /** Weight of a tsvector lexeme position. */
  public enum Weight {
    A,
    B,
    C,
    D
  }

  /** Compact constructor that makes an immutable copy of lexemes. */
  public Tsvector {
    lexemes = List.copyOf(lexemes);
  }

  /**
   * Normalizes a list of lexeme entries: sorts by token, deduplicates tokens (merging positions),
   * sorts positions within each lexeme, and deduplicates positions (keeping the minimum weight).
   */
  static Tsvector normalize(List<Lexeme> lexemes) {
    Map<String, List<Position>> merged = new TreeMap<>();
    for (Lexeme lex : lexemes) {
      merged.computeIfAbsent(lex.token(), k -> new ArrayList<>()).addAll(lex.positions());
    }
    List<Lexeme> result = new ArrayList<>();
    for (var entry : merged.entrySet()) {
      result.add(new Lexeme(entry.getKey(), deduplicatePositions(entry.getValue())));
    }
    return new Tsvector(result);
  }

  private static List<Position> deduplicatePositions(List<Position> positions) {
    if (positions.isEmpty()) {
      return List.of();
    }
    // Sort by position number, then by weight ordinal (A=0 < B=1 < C=2 < D=3)
    List<Position> sorted = new ArrayList<>(positions);
    sorted.sort(
        (a, b) -> {
          int cmp = Integer.compare(a.pos(), b.pos());
          return cmp != 0 ? cmp : a.weight().compareTo(b.weight());
        });
    // Deduplicate: for same position, keep the minimum weight
    List<Position> deduped = new ArrayList<>();
    Position prev = sorted.get(0);
    for (int i = 1; i < sorted.size(); i++) {
      Position cur = sorted.get(i);
      if (cur.pos() == prev.pos()) {
        // Keep the one with smaller weight ordinal (higher priority)
        if (cur.weight().compareTo(prev.weight()) < 0) {
          prev = cur;
        }
      } else {
        deduped.add(prev);
        prev = cur;
      }
    }
    deduped.add(prev);
    return Collections.unmodifiableList(deduped);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < lexemes.size(); i++) {
      if (i > 0) {
        sb.append(' ');
      }
      Lexeme lex = lexemes.get(i);
      sb.append('\'');
      sb.append(escapeTsvectorToken(lex.token()));
      sb.append('\'');
      if (!lex.positions().isEmpty()) {
        sb.append(':');
        for (int j = 0; j < lex.positions().size(); j++) {
          if (j > 0) {
            sb.append(',');
          }
          Position p = lex.positions().get(j);
          sb.append(p.pos());
          if (p.weight() != Weight.D) {
            sb.append(p.weight().name());
          }
        }
      }
    }
    return sb.toString();
  }

  static String escapeTsvectorToken(String token) {
    StringBuilder sb = new StringBuilder(token.length());
    for (int i = 0; i < token.length(); i++) {
      char c = token.charAt(i);
      if (c == '\'') {
        sb.append("''");
      } else if (c == '\\') {
        sb.append("\\\\");
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
