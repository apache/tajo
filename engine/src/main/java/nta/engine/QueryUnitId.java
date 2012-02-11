package nta.engine;

import java.text.NumberFormat;

/**
 * @author Hyunsik Choi
 */
public class QueryUnitId implements Comparable<QueryUnitId> {
  private static final NumberFormat format = NumberFormat.getInstance();
  static {
    format.setGroupingUsed(false);
    format.setMinimumIntegerDigits(6);
  }
  
  private final SubQueryId subQueryId;
  private final int id;
  private final String finalId;
  
  public QueryUnitId(final SubQueryId subQueryId, final int id) {
    this.subQueryId = subQueryId;
    this.id = id;
    this.finalId = this.subQueryId + QueryId.SEPERATOR + format.format(id);
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
