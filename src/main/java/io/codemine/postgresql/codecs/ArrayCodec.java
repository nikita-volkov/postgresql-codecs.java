package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Codec for multiple-dimensional arrays. */
final class ArrayCodec<A> implements Codec<List<A>> {

  private final Codec<A> elementCodec;
  private final char delimiter;

  public ArrayCodec(Codec<A> elementCodec) {
    this(elementCodec, ',');
  }

  public ArrayCodec(Codec<A> elementCodec, char delimiter) {
    this.elementCodec = elementCodec;
    this.delimiter = delimiter;
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

  @Override
  public String typeSig() {
    return elementCodec.typeSig() + "[]";
  }

  // -----------------------------------------------------------------------
  // Textual wire format
  // -----------------------------------------------------------------------
  @Override
  public void encodeInText(StringBuilder sb, List<A> value) {
    sb.append("{");
    for (int i = 0; i < value.size(); i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      A elem = value.get(i);
      if (elem == null) {
        // PostgreSQL represents null array elements as the unquoted literal NULL.
        sb.append("NULL");
        continue;
      }
      StringBuilder elemSb = new StringBuilder();
      elementCodec.encodeInText(elemSb, elem);
      int len = elemSb.length();
      if (len == 0) {
        sb.append("\"\"");
        continue;
      }
      boolean needsQuoting = false;
      // A bare "NULL" (case-insensitive) would be interpreted as a null element by PostgreSQL.
      // Quote it to preserve the literal string value.
      if (len == 4 && "NULL".equalsIgnoreCase(elemSb.toString())) {
        needsQuoting = true;
      }
      if (!needsQuoting) {
        for (int j = 0; j < len; j++) {
          char c = elemSb.charAt(j);
          if (c == delimiter
              || c == '{'
              || c == '}'
              || c == '"'
              || c == '\\'
              || Character.isWhitespace(c)) {
            needsQuoting = true;
            break;
          }
        }
      }
      if (needsQuoting) {
        sb.append('"');
        for (int j = 0; j < len; j++) {
          char c = elemSb.charAt(j);
          if (c == '"' || c == '\\') {
            sb.append('\\');
          }
          sb.append(c);
        }
        sb.append('"');
      } else {
        sb.append(elemSb);
      }
    }
    sb.append("}");
  }

  /**
   * Parses a PostgreSQL 1-D array literal ({@code {elem1,elem2,...}}) from {@code input} starting
   * at {@code offset}. Handles double-quoted elements (with backslash escaping) and bare elements.
   */
  @Override
  public ParsingResult<List<A>> decodeInText(CharSequence input, int offset)
      throws DecodingException {
    if (offset >= input.length() || input.charAt(offset) != '{') {
      throw new DecodingException(input, offset, "Expected '{' to open array literal");
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
          throw new DecodingException(input, offset, "Unterminated quoted array element");
        }
        pos++; // skip closing '"'
        list.add(elementCodec.decodeInText(elem.toString(), 0).value);
      } else {
        // Bare element: read until delimiter or '}'.
        int start = pos;
        while (pos < input.length() && input.charAt(pos) != delimiter && input.charAt(pos) != '}') {
          pos++;
        }
        String elemStr = input.subSequence(start, pos).toString();
        // In PostgreSQL array literals, an unquoted NULL denotes a null element.
        if ("NULL".equals(elemStr)) {
          list.add(null);
        } else {
          list.add(elementCodec.decodeInText(elemStr, 0).value);
        }
      }

      if (pos >= input.length()) {
        throw new DecodingException(input, offset, "Unexpected end of input parsing array");
      }
      char sep = input.charAt(pos);
      if (sep == '}') {
        return new ParsingResult<>(list, pos + 1);
      } else if (sep == delimiter) {
        pos++;
      } else {
        throw new DecodingException(
            input,
            offset,
            "Expected '" + delimiter + "' or '}' in array literal, got '" + sep + "'");
      }
    }
    throw new DecodingException(input, offset, "Unexpected end of input parsing array");
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
    writeInt32(out, dimensions()); // ndim
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

  /** Decodes a PostgreSQL binary array. */
  @Override
  public List<A> decodeInBinary(ByteBuffer buf, int length) throws DecodingException {
    int ndim = buf.getInt();
    buf.getInt(); // flags (ignored)
    int elementOid = buf.getInt();

    int expectedElementOid = elementCodec.scalarOid();

    if (expectedElementOid != 0 && elementOid != expectedElementOid) {
      throw new DecodingException(
          "Unexpected element OID in array binary decode: expected "
              + expectedElementOid
              + ", got "
              + elementOid);
    }

    if (ndim == 0) {
      return new ArrayList<>();
    }

    if (ndim != dimensions()) {
      throw new DecodingException(
          "Expected " + dimensions() + "-dimensional array in binary decode, got " + ndim);
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
  public List<A> random(Random r, int size) {
    if (size > 10) {
      size = 10; // prevent generating huge arrays
    }
    // Determine the inner size once and reuse it for every element.  This is critical for
    // multi-dimensional arrays: all sub-arrays at the same level must have the same length to
    // satisfy PostgreSQL's rectangular-array constraint.
    int innerSize = size == 0 ? 0 : r.nextInt(size + 1);
    List<A> list = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      list.add(elementCodec.random(r, innerSize));
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
