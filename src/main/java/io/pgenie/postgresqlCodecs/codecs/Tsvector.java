package io.pgenie.postgresqlCodecs.codecs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * PostgreSQL {@code tsvector} type. Full-text search document representation.
 *
 * <p>A tsvector is a sorted list of distinct lexemes with optional position and weight
 * information. Lexemes are stored in alphabetical order and deduplicated, matching
 * PostgreSQL's canonical representation.
 *
 * <p>Port of the Haskell {@code PostgresqlTypes.Tsvector} type.
 */
public final class Tsvector {

    /**
     * Weight of a tsvector lexeme position.
     *
     * <p>Port of the Haskell {@code PostgresqlTypes.Tsvector.Weight} enum.
     * Weights are ordered A (highest priority) through D (lowest/default).
     */
    public enum Weight {
        A, B, C, D;

        /** Encodes weight to the 2-bit field used in binary protocol (A=3, B=2, C=1, D=0). */
        int toBits() {
            return switch (this) {
                case A -> 3;
                case B -> 2;
                case C -> 1;
                case D -> 0;
            };
        }

        /** Decodes weight from the 2-bit field in binary protocol. */
        static Weight fromBits(int bits) {
            return switch (bits & 0x3) {
                case 3 -> A;
                case 2 -> B;
                case 1 -> C;
                default -> D;
            };
        }
    }

    /**
     * A position entry within a lexeme: a 1-based position (1–16383) and an optional weight.
     */
    public record Position(short pos, Weight weight) {}

    /**
     * The sorted lexeme entries: lexeme text → list of positions (sorted by pos ascending).
     * Uses a TreeMap to maintain lexicographic order.
     */
    private final List<Map.Entry<String, List<Position>>> lexemes;

    private Tsvector(List<Map.Entry<String, List<Position>>> lexemes) {
        this.lexemes = lexemes;
    }

    /**
     * Constructs a {@code Tsvector} from an ordered list of (lexeme, positions) entries.
     * Sorts and deduplicates lexemes to match PostgreSQL's canonical representation.
     *
     * @param entries  list of (lexeme text, positions) pairs
     * @return canonical Tsvector
     */
    public static Tsvector of(List<Map.Entry<String, List<Position>>> entries) {
        TreeMap<String, List<Position>> map = new TreeMap<>();
        for (var e : entries) {
            map.merge(e.getKey(), e.getValue(), (a, b) -> {
                var merged = new ArrayList<>(a);
                merged.addAll(b);
                return merged;
            });
        }
        List<Map.Entry<String, List<Position>>> sorted = new ArrayList<>();
        for (var e : map.entrySet()) {
            sorted.add(Map.entry(e.getKey(), normalizePositions(e.getValue())));
        }
        return new Tsvector(sorted);
    }

    /**
     * Returns the lexemes as an unmodifiable list of (lexeme, positions) entries.
     * Lexemes are in alphabetical order; positions within each lexeme are sorted
     * by position number ascending.
     */
    public List<Map.Entry<String, List<Position>>> toLexemeList() {
        return Collections.unmodifiableList(lexemes);
    }

    /**
     * Sorts positions ascending by pos and deduplicates, keeping the minimum weight
     * (highest priority) for duplicate positions.
     */
    static List<Position> normalizePositions(List<Position> positions) {
        if (positions.isEmpty()) return positions;
        var sorted = new ArrayList<>(positions);
        sorted.sort((a, b) -> Short.compareUnsigned(a.pos(), b.pos()));
        var result = new ArrayList<Position>();
        Position prev = sorted.get(0);
        for (int i = 1; i < sorted.size(); i++) {
            Position cur = sorted.get(i);
            if (cur.pos() == prev.pos()) {
                // keep min ordinal (A < B < C < D)
                prev = cur.weight().ordinal() < prev.weight().ordinal() ? cur : prev;
            } else {
                result.add(prev);
                prev = cur;
            }
        }
        result.add(prev);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tsvector that)) return false;
        return lexemes.equals(that.lexemes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lexemes);
    }

    @Override
    public String toString() {
        return "Tsvector" + lexemes;
    }

}
