package io.codemine.postgresql.codecs;

import java.util.List;

import net.jqwik.api.Group;

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
          (Integer x) -> (Integer y) -> new Point(x, y),
          new CompositeCodec.Field<>("x", Point::x, new Int4Codec()),
          new CompositeCodec.Field<>("y", Point::y, new Int4Codec()));

  static final CompositeCodec<Segment> SEGMENT_CODEC =
      new CompositeCodec<>(
          "",
          "test_seg",
          (Point start) -> (Point end) -> new Segment(start, end),
          new CompositeCodec.Field<>("start_pt", Segment::start, POINT_CODEC),
          new CompositeCodec.Field<>("end_pt", Segment::end, POINT_CODEC));

  static final CompositeCodec<TaggedData> TAGGED_DATA_CODEC =
      new CompositeCodec<>(
          "",
          "test_tagged",
          (String tag) -> (List<String> items) -> new TaggedData(tag, items),
          new CompositeCodec.Field<>("tag", TaggedData::tag, new TextCodec()),
          new CompositeCodec.Field<>("items", TaggedData::items, new TextCodec().inDim()));

  static final CompositeCodec<AnnotatedSegment> ANNOTATED_CODEC =
      new CompositeCodec<>(
          "",
          "test_ann_seg",
          (String label) ->
              (Segment seg) -> (List<String> tags) -> new AnnotatedSegment(label, seg, tags),
          new CompositeCodec.Field<>("label", AnnotatedSegment::label, new TextCodec()),
          new CompositeCodec.Field<>("seg", AnnotatedSegment::seg, SEGMENT_CODEC),
          new CompositeCodec.Field<>("tags", AnnotatedSegment::tags, new TextCodec().inDim()));

  // -----------------------------------------------------------------------
  // Test groups — each extends CodecTestBase to get the full binary+text
  // round-trip property suite.
  // -----------------------------------------------------------------------

  /** 2-field scalar composite: {@code (x int4, y int4)}. */
  @Group
  class SimplePointTests extends CodecTestBase<Point> {
    SimplePointTests() {
      super(POINT_CODEC, Point.class);
    }
  }

  /**
   * Composite whose fields are themselves composites: {@code (start test_pt, end test_pt)}.
   * Exercises nested composite encoding in both text and binary formats.
   */
  @Group
  class NestedSegmentTests extends CodecTestBase<Segment> {
    NestedSegmentTests() {
      super(SEGMENT_CODEC, Segment.class);
    }
  }

  /**
   * Composite with a scalar field and a 1-D array field: {@code (tag text, items text[])}.
   * Exercises array-within-composite encoding in both text and binary formats.
   */
  @Group
  class CompositeWithArrayTests extends CodecTestBase<TaggedData> {
    CompositeWithArrayTests() {
      super(TAGGED_DATA_CODEC, TaggedData.class);
    }
  }

  /**
   * Composite that nests another composite plus a 1-D array: {@code (label text, seg test_seg, tags
   * text[])}. Exercises the combination of nested composites and arrays.
   */
  @Group
  class NestedCompositeWithArrayTests extends CodecTestBase<AnnotatedSegment> {
    NestedCompositeWithArrayTests() {
      super(ANNOTATED_CODEC, AnnotatedSegment.class);
    }
  }
}
