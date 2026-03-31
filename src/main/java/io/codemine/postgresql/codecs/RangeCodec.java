package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Random;

/**
 * Codec for PostgreSQL range types. Delegates element encoding/decoding to the provided element
 * codec.
 *
 * <p>Supports all standard range types: {@code int4range}, {@code int8range}, {@code numrange},
 * {@code tsrange}, {@code tstzrange}, {@code daterange}.
 *
 * @param <A> the element type of the range
 */
final class RangeCodec<A> implements Codec<Range<A>> {

  private final Codec<A> elementCodec;
  private final Comparator<A> comparator;
  private final String typeName;
  private final int scalarOid;
  private final int arrayOid;

  RangeCodec(
      Codec<A> elementCodec,
      Comparator<A> comparator,
      String typeName,
      int scalarOid,
      int arrayOid) {
    this.elementCodec = elementCodec;
    this.comparator = comparator;
    this.typeName = typeName;
    this.scalarOid = scalarOid;
    this.arrayOid = arrayOid;
  }

  /** Returns the element codec. */
  Codec<A> elementCodec() {
    return elementCodec;
  }

  /** Returns the comparator used to order elements of this range. */
  Comparator<A> comparator() {
    return comparator;
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
  public void encodeInText(StringBuilder sb, Range<A> value) {
    switch (value) {
      case Range.Empty<?> e -> sb.append("empty");
      case Range.Bounded<A> b -> {
        if (b.lower() == null) {
          sb.append('(');
        } else {
          sb.append('[');
          elementCodec.encodeInText(sb, b.lower());
        }
        sb.append(',');
        if (b.upper() != null) {
          elementCodec.encodeInText(sb, b.upper());
        }
        sb.append(')');
      }
    }
  }

  @Override
  public Codec.ParsingResult<Range<A>> decodeInText(CharSequence input, int offset)
      throws Codec.DecodingException {
    if (offset >= input.length()) {
      throw new Codec.DecodingException(input, offset, "Unexpected end of range input");
    }

    // Check for "empty"
    if (input.length() - offset >= 5
        && "empty".contentEquals(input.subSequence(offset, offset + 5))) {
      return new Codec.ParsingResult<>(Range.empty(), offset + 5);
    }

    int pos = offset;
    char openBracket = input.charAt(pos);
    if (openBracket != '[' && openBracket != '(') {
      throw new Codec.DecodingException(input, offset, "Expected '[' or '(' to open range literal");
    }
    pos++;

    // Parse lower bound
    A lower = null;
    if (openBracket == '[') {
      // Inclusive lower bound — parse the element value
      // Need to handle possible quoted values
      if (pos < input.length() && input.charAt(pos) == '"') {
        pos++; // skip opening quote
        StringBuilder unquoted = new StringBuilder();
        while (pos < input.length() && input.charAt(pos) != '"') {
          if (input.charAt(pos) == '\\' && pos + 1 < input.length()) {
            unquoted.append(input.charAt(pos + 1));
            pos += 2;
          } else {
            unquoted.append(input.charAt(pos));
            pos++;
          }
        }
        if (pos < input.length()) {
          pos++; // skip closing quote
        }
        lower = elementCodec.decodeInTextFromString(unquoted.toString());
      } else {
        // Parse until comma
        int commaPos = findComma(input, pos);
        if (commaPos < 0) {
          throw new Codec.DecodingException(input, offset, "Missing ',' in range literal");
        }
        String lowerStr = input.subSequence(pos, commaPos).toString();
        lower = elementCodec.decodeInTextFromString(lowerStr);
        pos = commaPos;
      }
    }
    // openBracket == '(' means lower is infinite (null), skip to comma

    // Expect comma
    if (pos >= input.length()) {
      throw new Codec.DecodingException(input, offset, "Unexpected end of range input");
    }
    if (input.charAt(pos) == ',') {
      pos++;
    } else {
      throw new Codec.DecodingException(input, offset, "Expected ',' in range literal");
    }

    // Parse upper bound
    A upper = null;
    if (pos < input.length() && input.charAt(pos) != ')') {
      // There's an upper bound
      if (input.charAt(pos) == '"') {
        pos++; // skip opening quote
        StringBuilder unquoted = new StringBuilder();
        while (pos < input.length() && input.charAt(pos) != '"') {
          if (input.charAt(pos) == '\\' && pos + 1 < input.length()) {
            unquoted.append(input.charAt(pos + 1));
            pos += 2;
          } else {
            unquoted.append(input.charAt(pos));
            pos++;
          }
        }
        if (pos < input.length()) {
          pos++; // skip closing quote
        }
        upper = elementCodec.decodeInTextFromString(unquoted.toString());
      } else {
        int closePos = findCloseParen(input, pos);
        if (closePos < 0) {
          throw new Codec.DecodingException(input, offset, "Missing ')' in range literal");
        }
        String upperStr = input.subSequence(pos, closePos).toString();
        upper = elementCodec.decodeInTextFromString(upperStr);
        pos = closePos;
      }
    }

    // Expect closing paren
    if (pos >= input.length() || input.charAt(pos) != ')') {
      throw new Codec.DecodingException(input, offset, "Expected ')' to close range literal");
    }
    pos++;

    return new Codec.ParsingResult<>(Range.bounded(lower, upper), pos);
  }

  private static int findComma(CharSequence input, int from) {
    for (int i = from; i < input.length(); i++) {
      if (input.charAt(i) == ',') {
        return i;
      }
    }
    return -1;
  }

  private static int findCloseParen(CharSequence input, int from) {
    for (int i = from; i < input.length(); i++) {
      if (input.charAt(i) == ')') {
        return i;
      }
    }
    return -1;
  }

  // -----------------------------------------------------------------------
  // Binary wire format
  // -----------------------------------------------------------------------

  // Flags byte layout:
  // bit 0: RANGE_EMPTY
  // bit 1: RANGE_LB_INC (lower bound inclusive)
  // bit 2: RANGE_UB_INC (upper bound inclusive)
  // bit 3: RANGE_LB_INF (lower bound infinite)
  // bit 4: RANGE_UB_INF (upper bound infinite)
  private static final int RANGE_EMPTY = 0x01;
  private static final int RANGE_LB_INC = 0x02;
  private static final int RANGE_LB_INF = 0x08;
  private static final int RANGE_UB_INF = 0x10;

  @Override
  public void encodeInBinary(Range<A> value, ByteArrayOutputStream out) {
    switch (value) {
      case Range.Empty<?> e -> out.write(RANGE_EMPTY);
      case Range.Bounded<A> b -> {
        int flags = 0;
        if (b.lower() == null) {
          flags |= RANGE_LB_INF;
        } else {
          flags |= RANGE_LB_INC; // lower is always inclusive in normalized form
        }
        if (b.upper() == null) {
          flags |= RANGE_UB_INF;
        }
        // upper is always exclusive in normalized form, so no RANGE_UB_INC
        out.write(flags);
        if (b.lower() != null) {
          byte[] lowerBytes = elementCodec.encodeInBinaryToBytes(b.lower());
          writeInt32(out, lowerBytes.length);
          out.write(lowerBytes, 0, lowerBytes.length);
        }
        if (b.upper() != null) {
          byte[] upperBytes = elementCodec.encodeInBinaryToBytes(b.upper());
          writeInt32(out, upperBytes.length);
          out.write(upperBytes, 0, upperBytes.length);
        }
      }
    }
  }

  @Override
  public Range<A> decodeInBinary(ByteBuffer buf, int length) throws Codec.DecodingException {
    int flags = buf.get() & 0xFF;
    if ((flags & RANGE_EMPTY) != 0) {
      return Range.empty();
    }

    boolean lowerInfinite = (flags & RANGE_LB_INF) != 0;
    boolean upperInfinite = (flags & RANGE_UB_INF) != 0;

    A lower = null;
    if (!lowerInfinite) {
      int lowerLen = buf.getInt();
      lower = elementCodec.decodeInBinary(buf, lowerLen);
    }

    A upper = null;
    if (!upperInfinite) {
      int upperLen = buf.getInt();
      upper = elementCodec.decodeInBinary(buf, upperLen);
    }

    return Range.bounded(lower, upper);
  }

  // -----------------------------------------------------------------------
  // Random generation
  // -----------------------------------------------------------------------
  @Override
  public Range<A> random(Random r, int size) {
    if (size == 0) {
      return Range.empty();
    }
    // 10% chance of empty, 10% chance of fully-unbounded, 80% bounded
    int choice = r.nextInt(10);
    if (choice == 0) {
      return Range.empty();
    }
    if (choice == 1) {
      return Range.unbounded();
    }

    boolean lowerInfinite = r.nextInt(10) == 0;
    boolean upperInfinite = r.nextInt(10) == 0;

    A lower = lowerInfinite ? null : elementCodec.random(r, size);
    A upper = upperInfinite ? null : elementCodec.random(r, size);

    // Ensure lower <= upper; if equal, canonicalize to empty.
    if (!lowerInfinite && !upperInfinite) {
      int cmp = comparator.compare(lower, upper);
      if (cmp == 0) {
        return Range.empty();
      }
      if (cmp > 0) {
        // Swap so that lower < upper.
        A tmp = lower;
        lower = upper;
        upper = tmp;
      }
    }

    return Range.bounded(lower, upper);
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
