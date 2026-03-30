package io.codemine.postgresql.codecs;

import java.util.List;

/**
 * PostgreSQL {@code path} type. A geometric path consisting of a list of points that may be open or
 * closed.
 *
 * @param closed {@code true} for a closed path, {@code false} for an open path
 * @param points the vertices of the path
 */
public record Path(boolean closed, List<Point> points) {}
