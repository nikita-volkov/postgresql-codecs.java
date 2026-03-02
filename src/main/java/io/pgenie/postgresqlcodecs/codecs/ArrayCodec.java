package io.pgenie.postgresqlcodecs.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Codec for multiple-dimensional arrays. */
public final class ArrayCodec<A> implements Codec<List<A>> {

  private final Codec<A> elementCodec;

  public ArrayCodec(Codec<A> elementCodec) {
    this.elementCodec = elementCodec;
  }

  @Override
  public String schema() {
    return elementCodec.schema();
  }

  @Override
  public String name() {
    return elementCodec.name();
  }

  @Override
  public int dimensions() {
    return elementCodec.dimensions() + 1;
  }

  @Override
  public int scalarOid() {
    return elementCodec.scalarOid();
  }

  @Override
  public int arrayOid() {
    return elementCodec.arrayOid();
  }

  // -----------------------------------------------------------------------
  // Textual wire format
  // -----------------------------------------------------------------------
  @Override
  public void write(StringBuilder sb, List<A> value) {
    sb.append("{");
    for (int i = 0; i < value.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      elementCodec.write(sb, value.get(i));
    }
    sb.append("}");
  }

  /**
   * Parses a PostgreSQL 1-D array literal ({@code {elem1,elem2,...}}) from {@code input} starting
   * at {@code offset}. Handles double-quoted elements (with backslash escaping) and bare elements.
   */
  @Override
  public ParsingResult<List<A>> parse(CharSequence input, int offset) throws ParseException {
    if (offset >= input.length() || input.charAt(offset) != '{') {
      throw new ParseException(input, offset, "Expected '{' to open array literal");
    }
    int pos = offset + 1;
    List<A> list = new ArrayList<>();

    // Empty array
    if (pos < input.length() && input.charAt(pos) == '}') {
      return new ParsingResult<>(list, pos + 1);
    }

    while (pos < input.length()) {
      char c = input.charAt(pos);

      if (c == '"') {
        // Quoted element: collect raw chars between double-quotes, honouring backslash escapes.
        pos++; // skip opening '"'
        StringBuilder elem = new StringBuilder();
        while (pos < input.length()) {
          char ec = input.charAt(pos);
          if (ec == '\\' && pos + 1 < input.length()) {
            elem.append(input.charAt(pos + 1));
            pos += 2;
          } else if (ec == '"') {
            break;
          } else {
            elem.append(ec);
            pos++;
          }
        }
        if (pos >= input.length() || input.charAt(pos) != '"') {
          throw new ParseException(input, offset, "Unterminated quoted array element");
        }
        pos++; // skip closing '"'
        list.add(elementCodec.parse(elem.toString(), 0).value);
      } else {
        // Bare element: read until ',' or '}'.
        int start = pos;
        while (pos < input.length() && input.charAt(pos) != ',' && input.charAt(pos) != '}') {
          pos++;
        }
        String elemStr = input.subSequence(start, pos).toString();
        list.add(elementCodec.parse(elemStr, 0).value);
      }

      if (pos >= input.length()) {
        throw new ParseException(input, offset, "Unexpected end of input parsing array");
      }
      char sep = input.charAt(pos);
      if (sep == '}') {
        return new ParsingResult<>(list, pos + 1);
      } else if (sep == ',') {
        pos++;
      } else {
        throw new ParseException(
            input, offset, "Expected ',' or '}' in array literal, got '" + sep + "'");
      }
    }
    throw new ParseException(input, offset, "Unexpected end of input parsing array");
  }

  // -----------------------------------------------------------------------
  // Binary wire format
  // -----------------------------------------------------------------------
  /**
   * Encodes a 1-D array in PostgreSQL binary array format.
   *
   * <pre>
   *   int32  ndim        = 1
   *   int32  flags       = 0
   *   int32  element_oid
   *   int32  dim_size
   *   int32  dim_lbound  = 1
   *   for each element:
   *     int32  elem_length  (-1 for NULL)
   *     byte[] elem_data
   * </pre>
   */
  @Override
  public void encodeInBinary(List<A> value, ByteArrayOutputStream out) {
    writeInt32(out, 1); // ndim
    writeInt32(out, 0); // flags
    writeInt32(out, elementCodec.scalarOid()); // element OID
    writeInt32(out, value.size()); // dimension size
    writeInt32(out, 1); // lower bound (PostgreSQL convention: 1-based)
    for (A elem : value) {
      if (elem == null) {
        writeInt32(out, -1);
      } else {
        ByteArrayOutputStream elemOut = new ByteArrayOutputStream();
        elementCodec.encodeInBinary(elem, elemOut);
        byte[] bytes = elemOut.toByteArray();
        writeInt32(out, bytes.length);
        out.writeBytes(bytes);
      }
    }
  }

  /**
   * Decodes a PostgreSQL binary array. Expects ndim == 1 (or 0 for empty); multi-dimensional arrays
   * are rejected.
   */
  @Override
  public List<A> decodeInBinary(ByteBuffer buf, int length) throws ParseException {
    int ndim = buf.getInt();
    buf.getInt(); // flags (ignored)
    buf.getInt(); // element OID (ignored; we trust the connection)
    if (ndim == 0) {
      return new ArrayList<>();
    }
    if (ndim != 1) {
      throw new ParseException("Expected 1-dimensional array in binary decode, got ndim=" + ndim);
    }
    int dimSize = buf.getInt();
    buf.getInt(); // lower bound (ignored)
    List<A> result = new ArrayList<>(dimSize);
    for (int i = 0; i < dimSize; i++) {
      int elemLen = buf.getInt();
      if (elemLen == -1) {
        result.add(null);
      } else {
        result.add(elementCodec.decodeInBinary(buf, elemLen));
      }
    }
    return result;
  }

  // -----------------------------------------------------------------------
  // Random generation
  // -----------------------------------------------------------------------
  @Override
  public List<A> random(Random r) {
    int size = r.nextInt(6); // 0–5 elements
    List<A> list = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      list.add(elementCodec.random(r));
    }
    return list;
  }

  // -----------------------------------------------------------------------
  // Helper
  // -----------------------------------------------------------------------
  private void writeInt32(ByteArrayOutputStream out, int v) {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 8) & 0xFF);
    out.write(v & 0xFF);
  }
}
