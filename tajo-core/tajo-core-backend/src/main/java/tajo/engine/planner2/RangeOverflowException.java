package tajo.engine.planner2;

import tajo.storage.Tuple;
import tajo.storage.TupleRange;

/**
 * @author
 */
public class RangeOverflowException extends RuntimeException {
  public RangeOverflowException(TupleRange range, Tuple overflowValue, long inc) {
    super("Overflow Error: tried to increase " + inc + " to " + overflowValue + ", but the range " + range);
  }
}
