package io.codemine.postgresql.codecs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
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
  // Minimal scalar codecs used throughout
  // -----------------------------------------------------------------------

  /** PostgreSQL {@code int4} codec. */
  static final Codec<Integer> INT4 =
      new Codec<>() {
        @Override
        public String name() {
          return "int4";
        }

        @Override
        public int scalarOid() {
          return 23;
        }

        @Override
        public int arrayOid() {
          return 1007;
        }

        @Override
        public void write(StringBuilder sb, Integer value) {
          sb.append(value);
        }

        @Override
        public Codec.ParsingResult<Integer> parse(CharSequence input, int offset)
            throws Codec.ParseException {
          try {
            int v = Integer.parseInt(input.subSequence(offset, input.length()).toString().trim());
            return new Codec.ParsingResult<>(v, input.length());
          } catch (NumberFormatException e) {
            throw new Codec.ParseException(input, offset, "Invalid int4: " + e.getMessage());
          }
        }

        @Override
        public void encodeInBinary(Integer value, ByteArrayOutputStream out) {
          out.write((value >>> 24) & 0xFF);
          out.write((value >>> 16) & 0xFF);
          out.write((value >>> 8) & 0xFF);
          out.write(value & 0xFF);
        }

        @Override
        public Integer decodeInBinary(ByteBuffer buf, int length) {
          return buf.getInt();
        }

        @Override
        public Integer random(Random r) {
          // Keep values small so they print without special chars
          return r.nextInt(10_000) - 5_000;
        }
      };

  /** PostgreSQL {@code text} codec. Produces raw text; the composite codec handles quoting. */
  static final Codec<String> TEXT =
      new Codec<>() {
        @Override
        public String name() {
          return "text";
        }

        @Override
        public int scalarOid() {
          return 25;
        }

        @Override
        public int arrayOid() {
          return 1009;
        }

        @Override
        public void write(StringBuilder sb, String value) {
          sb.append(value);
        }

        @Override
        public Codec.ParsingResult<String> parse(CharSequence input, int offset) {
          return new Codec.ParsingResult<>(
              input.subSequence(offset, input.length()).toString(), input.length());
        }

        @Override
        public void encodeInBinary(String value, ByteArrayOutputStream out) {
          byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
          out.write(bytes, 0, bytes.length);
        }

        @Override
        public String decodeInBinary(ByteBuffer buf, int length) {
          byte[] bytes = new byte[length];
          buf.get(bytes);
          return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public String random(Random r) {
          // Include special chars to exercise composite quoting/escaping paths
          String chars = "abcdefghijklmnopqABCDEF0123,()\"\\";
          int len = r.nextInt(10);
          StringBuilder sb = new StringBuilder(len);
          for (int i = 0; i < len; i++) {
            sb.append(chars.charAt(r.nextInt(chars.length())));
          }
          return sb.toString();
        }
      };

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
          new CompositeCodec.Field<>("x", Point::x, INT4),
          new CompositeCodec.Field<>("y", Point::y, INT4));

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
          new CompositeCodec.Field<>("tag", TaggedData::tag, TEXT),
          new CompositeCodec.Field<>("items", TaggedData::items, TEXT.inDim()));

  static final CompositeCodec<AnnotatedSegment> ANNOTATED_CODEC =
      new CompositeCodec<>(
          "",
          "test_ann_seg",
          (String label) ->
              (Segment seg) -> (List<String> tags) -> new AnnotatedSegment(label, seg, tags),
          new CompositeCodec.Field<>("label", AnnotatedSegment::label, TEXT),
          new CompositeCodec.Field<>("seg", AnnotatedSegment::seg, SEGMENT_CODEC),
          new CompositeCodec.Field<>("tags", AnnotatedSegment::tags, TEXT.inDim()));

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
