package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * Codec for PostgreSQL multirange types. Delegates range encoding/decoding to the provided range
 * codec.
 *
 * @param <A> the element type of the ranges within the multirange
 */
final class MultirangeCodec<A> implements Codec<Multirange<A>> {

  private final Codec<Range<A>> rangeCodec;
  private final Codec<A> elementCodec;
  private final Comparator<A> comparator;
  private final String typeName;
  private final int scalarOid;
  private final int arrayOid;

  MultirangeCodec(
      Codec<Range<A>> rangeCodec,
      Codec<A> elementCodec,
      Comparator<A> comparator,
      String typeName,
      int scalarOid,
      int arrayOid) {
    this.rangeCodec = rangeCodec;
    this.elementCodec = elementCodec;
    this.comparator = comparator;
    this.typeName = typeName;
    this.scalarOid = scalarOid;
    this.arrayOid = arrayOid;
  }

  @Override
  public String name() {
    return typeName;
  }

  @Override
  public int scalarOid() {
    return scalarOid;
  }

  @Override
  public int arrayOid() {
    return arrayOid;
  }

  // -----------------------------------------------------------------------
  // Textual wire format
  // -----------------------------------------------------------------------
  @Override
  public void encodeInText(StringBuilder sb, Multirange<A> value) {
    sb.append('{');
    for (int i = 0; i < value.ranges().size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      rangeCodec.encodeInText(sb, value.ranges().get(i));
    }
    sb.append('}');
  }

  @Override
  public Codec.ParsingResult<Multirange<A>> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    if (offset >= input.length() || input.charAt(offset) != '{') {
      throw new Codec.DecodingException(input, offset, "Expected '{' to open multirange literal");
    }
    int pos = offset + 1;
    // Skip whitespace
    while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
      pos++;
    }
    List<Range<A>> ranges = new ArrayList<>();
    if (pos < input.length() && input.charAt(pos) == '}') {
      return new Codec.ParsingResult<>(new Multirange<>(ranges), pos + 1);
    }
    while (pos < input.length()) {
      // Skip whitespace
      while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
        pos++;
      }
      if (pos >= input.length()) {
        break;
      }
      if (input.charAt(pos) == '}') {
        return new Codec.ParsingResult<>(new Multirange<>(ranges), pos + 1);
      }
      Codec.ParsingResult<Range<A>> result = rangeCodec.decodeInText(input, pos);
      ranges.add(result.value);
      pos = result.nextOffset;
      // Skip comma separator
      while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
        pos++;
      }
      if (pos < input.length() && input.charAt(pos) == ',') {
        pos++;
      }
    }
    throw new Codec.DecodingException(input, offset, "Unexpected end of input parsing multirange");
  }

  // -----------------------------------------------------------------------
  // Binary wire format
  // -----------------------------------------------------------------------
  @Override
  public void encodeInBinary(Multirange<A> value, ByteArrayOutputStream out) {
    // 4 bytes: number of ranges
    writeInt32(out, value.ranges().size());
    for (Range<A> range : value.ranges()) {
      byte[] rangeBytes = rangeCodec.encodeInBinaryToBytes(range);
      writeInt32(out, rangeBytes.length);
      out.write(rangeBytes, 0, rangeBytes.length);
    }
  }

  @Override
  public Multirange<A> decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    int numRanges = buf.getInt();
    if (numRanges < 0) {
      throw new Codec.DecodingException("Negative range count in multirange binary data");
    }
    List<Range<A>> ranges = new ArrayList<>(numRanges);
    for (int i = 0; i < numRanges; i++) {
      int rangeLen = buf.getInt();
      ranges.add(rangeCodec.decodeInBinary(buf, rangeLen));
    }
    return new Multirange<>(ranges);
  }

  // -----------------------------------------------------------------------
  // Random generation
  // -----------------------------------------------------------------------
  @Override
  public Multirange<A> random(Random r, int size) {
    if (size == 0) {
      return new Multirange<>(List.of());
    }

    // Generate up to 3 non-overlapping, non-adjacent ranges.
    //
    // Strategy: generate a pool of distinct sorted candidate values, then
    // take pairs with a one-value gap between successive ranges:
    //   range0 = [cands[0], cands[1])
    //   range1 = [cands[3], cands[4])   ← cands[2] acts as the gap
    //   ...
    // Because the candidates are strictly sorted (duplicates removed), each
    // range's upper bound is less than the next range's lower bound, which
    // guarantees PostgreSQL won't merge or reorder them.
    int numRanges = r.nextInt(3) + 1;
    int needed = numRanges * 2 + (numRanges - 1); // 2 bounds per range + gap between ranges

    // Generate a generous pool so there are enough distinct values after dedup.
    int poolSize = needed * 4;
    List<A> pool = new ArrayList<>(poolSize);
    for (int i = 0; i < poolSize; i++) {
      pool.add(elementCodec.random(r, size));
    }

    // Sort and remove consecutive duplicates.
    pool.sort(comparator);
    List<A> distinct = new ArrayList<>(poolSize);
    for (A v : pool) {
      if (distinct.isEmpty() || comparator.compare(distinct.get(distinct.size() - 1), v) != 0) {
        distinct.add(v);
      }
    }

    // Build ranges from the sorted distinct values.
    List<Range<A>> ranges = new ArrayList<>(numRanges);
    int idx = 0;
    while (ranges.size() < numRanges && idx + 1 < distinct.size()) {
      ranges.add(Range.bounded(distinct.get(idx), distinct.get(idx + 1)));
      idx += 3; // skip one gap value before the next range
    }

    return new Multirange<>(ranges);
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
}
