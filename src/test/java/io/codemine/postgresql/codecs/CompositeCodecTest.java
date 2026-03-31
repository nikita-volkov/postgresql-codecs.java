package io.codemine.postgresql.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.List;
import net.jqwik.api.Group;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CompositeCodec}, covering text and binary round-trips for four composite
 * scenarios:
 *
 * <ol>
 *   <li>{@link SimplePointTests} – 2-field scalar composite {@code (x int4, y int4)}
 *   <li>{@link NestedSegmentTests} – composite containing two composites {@code (start Point, end
 *       Point)}
 *   <li>{@link CompositeWithArrayTests} – composite with an array field {@code (tag text, items
 *       text[])}
 *   <li>{@link NestedCompositeWithArrayTests} – composite containing a composite plus an array
 *       {@code (label text, seg Segment, tags text[])}
 * </ol>
 */
class CompositeCodecTest {

  // -----------------------------------------------------------------------
  // Test value types (records)
  // -----------------------------------------------------------------------

  record Point(int x, int y) {}

  record Segment(Point start, Point end) {}

  record TaggedData(String tag, List<String> items) {}

  record AnnotatedSegment(String label, Segment seg, List<String> tags) {}

  // -----------------------------------------------------------------------
  // Composite codecs
  // -----------------------------------------------------------------------

  static final CompositeCodec<Point> POINT_CODEC =
      new CompositeCodec<>(
          "",
          "test_pt",
          args -> new Point((Integer) args[0], (Integer) args[1]),
          new CompositeCodec.Field<>("x", Point::x, Codec.INT4),
          new CompositeCodec.Field<>("y", Point::y, Codec.INT4));

  static final CompositeCodec<Segment> SEGMENT_CODEC =
      new CompositeCodec<>(
          "",
          "test_seg",
          args -> new Segment((Point) args[0], (Point) args[1]),
          new CompositeCodec.Field<>("start_pt", Segment::start, POINT_CODEC),
          new CompositeCodec.Field<>("end_pt", Segment::end, POINT_CODEC));

  @SuppressWarnings("unchecked")
  static final CompositeCodec<TaggedData> TAGGED_DATA_CODEC =
      new CompositeCodec<>(
          "",
          "test_tagged",
          args -> new TaggedData((String) args[0], (List<String>) args[1]),
          new CompositeCodec.Field<>("tag", TaggedData::tag, Codec.TEXT),
          new CompositeCodec.Field<>("items", TaggedData::items, Codec.TEXT.inDim()));

  @SuppressWarnings("unchecked")
  static final CompositeCodec<AnnotatedSegment> ANNOTATED_CODEC =
      new CompositeCodec<>(
          "",
          "test_ann_seg",
          args -> new AnnotatedSegment((String) args[0], (Segment) args[1], (List<String>) args[2]),
          new CompositeCodec.Field<>("label", AnnotatedSegment::label, Codec.TEXT),
          new CompositeCodec.Field<>("seg", AnnotatedSegment::seg, SEGMENT_CODEC),
          new CompositeCodec.Field<>("tags", AnnotatedSegment::tags, Codec.TEXT.inDim()));

  @Test
  void binaryDecodeRejectsUnexpectedScalarFieldOid() {
    Point value = new Point(1, 2);
    byte[] encoded = POINT_CODEC.encodeInBinaryToBytes(value);
    ByteBuffer.wrap(encoded).putInt(Integer.BYTES, Codec.TEXT.oid());

    var error =
        assertThrows(
            Codec.DecodingException.class,
            () -> POINT_CODEC.decodeInBinary(ByteBuffer.wrap(encoded), encoded.length));

    assertEquals(
        "Unexpected field OID in composite binary decode for field 'x' of test_pt: expected "
            + Codec.INT4.oid()
            + ", got "
            + Codec.TEXT.oid(),
        error.getMessage());
  }

  @Test
  void binaryEncodeUsesArrayOidForArrayField() {
    TaggedData value = new TaggedData("tag", List.of("alpha"));
    ByteBuffer buf = ByteBuffer.wrap(TAGGED_DATA_CODEC.encodeInBinaryToBytes(value));

    assertEquals(2, buf.getInt());
    buf.getInt();
    int firstFieldLength = buf.getInt();
    buf.position(buf.position() + firstFieldLength);

    assertEquals(Codec.TEXT.inDim().oid(), buf.getInt());
  }

  @Test
  void binaryDecodeAcceptsNonzeroFieldOidWhenTypeOidIsUnknown() throws Exception {
    Segment value = new Segment(new Point(1, 2), new Point(3, 4));
    byte[] encoded = SEGMENT_CODEC.encodeInBinaryToBytes(value);
    int serverAssignedOid = 12_345;
    ByteBuffer.wrap(encoded).putInt(Integer.BYTES, serverAssignedOid);

    assertEquals(value, SEGMENT_CODEC.decodeInBinary(ByteBuffer.wrap(encoded), encoded.length));
  }

  // -----------------------------------------------------------------------
  // Test groups — each extends CodecTestBase to get the full binary+text
  // round-trip property suite.
  // -----------------------------------------------------------------------

  /** 2-field scalar composite: {@code (x int4, y int4)}. */
  @Group
  class SimplePointTests extends CodecTestBase<Point> {
    SimplePointTests() {
      super(POINT_CODEC);
    }
  }

  /**
   * Composite whose fields are themselves composites: {@code (start test_pt, end test_pt)}.
   * Exercises nested composite encoding in both text and binary formats.
   */
  @Group
  class NestedSegmentTests extends CodecTestBase<Segment> {
    NestedSegmentTests() {
      super(SEGMENT_CODEC);
    }
  }

  /**
   * Composite with a scalar field and a 1-D array field: {@code (tag text, items text[])}.
   * Exercises array-within-composite encoding in both text and binary formats.
   */
  @Group
  class CompositeWithArrayTests extends CodecTestBase<TaggedData> {
    CompositeWithArrayTests() {
      super(TAGGED_DATA_CODEC);
    }
  }

  /**
   * Composite that nests another composite plus a 1-D array: {@code (label text, seg test_seg, tags
   * text[])}. Exercises the combination of nested composites and arrays.
   */
  @Group
  class NestedCompositeWithArrayTests extends CodecTestBase<AnnotatedSegment> {
    NestedCompositeWithArrayTests() {
      super(ANNOTATED_CODEC);
    }
  }
}
