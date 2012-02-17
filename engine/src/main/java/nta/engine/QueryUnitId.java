package nta.engine;

import java.text.NumberFormat;
import java.util.StringTokenizer;

/**
 * @author Hyunsik Choi
 */
public class QueryUnitId implements Comparable<QueryUnitId> {
  private static final NumberFormat format = NumberFormat.getInstance();
  static {
    format.setGroupingUsed(false);
    format.setMinimumIntegerDigits(6);
  }
  
  private final QueryStepId queryStepId;
  private final int id;
  private final String finalId;
  
  public QueryUnitId(final QueryStepId queryStepId, final int id) {
    this.queryStepId = queryStepId;
    this.id = id;
    this.finalId = this.queryStepId + QueryId.SEPERATOR + format.format(id);
  }
  
  public QueryUnitId(final String finalId) {
    this.queryStepId = null;
    this.id = -1;
    this.finalId = finalId;
  }
  
  @Override
  public final String toString() {
    return this.finalId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof QueryUnitId) {
      QueryUnitId other = (QueryUnitId) o;
      return this.finalId.equals(other.finalId);
    }    
    return false;
  }
  
  @Override
  public int hashCode() {
    return finalId.hashCode();
  }

  @Override
  public final int compareTo(final QueryUnitId o) {
    return this.finalId.compareTo(o.finalId);
  }
}
