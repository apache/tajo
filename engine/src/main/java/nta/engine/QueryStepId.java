package nta.engine;

import java.text.NumberFormat;

public class QueryStepId implements Comparable<QueryStepId> {
  
private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(3);
  }

  public static String SEPERATOR = "_";
  private SubQueryId subQueryId;
  private int id;
  private String finalId;
  
  public QueryStepId(SubQueryId subQueryId, final int id) {
    this.subQueryId = subQueryId;
    this.id = id;
    finalId = subQueryId +  SEPERATOR + idFormat.format(id);
  }
  
  public QueryStepId(String finalId) {
    this.finalId = finalId;
    int i = finalId.lastIndexOf(QueryId.SEPERATOR);
    subQueryId = new SubQueryId(finalId.substring(0, i));
    id = Integer.valueOf(finalId.substring(i+1));
  }
  
  public int getId() {
    return this.id;
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
