package tajo.storage;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import tajo.catalog.Schema;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.physical.TupleComparator;

import java.util.Comparator;

/**
 * @author Hyunsik Choi
 */
public class TupleRange implements Comparable<TupleRange> {
  private final Schema schema;
  private final Tuple start;
  private final Tuple end;
  private final TupleComparator comp;

  public TupleRange(final Schema schema, final Tuple start, final Tuple end) {
    this.comp = new TupleComparator(schema, PlannerUtil.schemaToSortSpecs(schema));
    // if there is only one value, start == end
    Preconditions.checkArgument(comp.compare(start, end) <= 0, ("start=" + start) + ", end=" + end);
    this.schema = schema;
    this.start = start;
    this.end = end;
  }

  public final Schema getSchema() {
    return this.schema;
  }

  public final Tuple getStart() {
    return this.start;
  }

  public final Tuple getEnd() {
    return this.end;
  }

  public String toString() {
    return "[" + this.start + ", " + this.end+")";
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(start, end);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof  TupleRange) {
      TupleRange other = (TupleRange) obj;
      return this.start.equals(other.start) && this.end.equals(other.end);
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(TupleRange o) {
    // TODO - should handle overlap
    int cmpVal = comp.compare(this.start, o.start);
    if (cmpVal != 0) {
      return cmpVal;
    } else {
      return comp.compare(this.end, o.end);
    }
  }

  public static class DescendingTupleRangeComparator
      implements Comparator<TupleRange> {

    @Override
    public int compare(TupleRange left, TupleRange right) {
      return -(left.compareTo(right));
    }
  }
}
