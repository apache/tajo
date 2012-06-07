package nta.storage;

/**
 * @author Hyunsik Choi
 */
public class TupleRange {
  private final Tuple start;
  private final Tuple end;

  public TupleRange(final Tuple start, final Tuple end) {
    this.start = start;
    this.end = end;
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
}
