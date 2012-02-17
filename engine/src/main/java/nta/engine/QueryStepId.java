package nta.engine;

import java.text.NumberFormat;

public class QueryStepId implements Comparable<QueryStepId> {
  
private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(3);
  }

  public static final String SEPERATOR = "_";
  private final SubQueryId subQueryId;
  private final String finalId;
  
  public QueryStepId(SubQueryId subQueryId, final long id) {
    this.subQueryId = subQueryId;
    finalId = subQueryId +  SEPERATOR + idFormat.format(id);
  }

  @Override
  public int compareTo(QueryStepId o) {
    return this.finalId.compareTo(o.finalId);
  }

  @Override
  public final String toString() {
    return finalId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof QueryStepId) {
      QueryStepId sid = (QueryStepId) o;
      return this.finalId.equals(sid.finalId);
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.finalId.hashCode();
  }
  
  public SubQueryId getSubQueryId() {
    return this.subQueryId;
  }
  
}
