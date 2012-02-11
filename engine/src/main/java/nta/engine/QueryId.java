/**
 * 
 */
package nta.engine;

import java.text.NumberFormat;

/**
 * @author Hyunsik Choi
 */
public class QueryId implements Comparable<QueryId> {
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(3);
  }

  private static final String PREFIX = "query";
  public static final String SEPERATOR = "_";
  private final String finalId;
  
  public QueryId(final String timeId, final long id) {
    finalId = PREFIX + SEPERATOR + timeId +  SEPERATOR + idFormat.format(id);
  }
  
  public final String toString() {
    return finalId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof QueryId) {
      QueryId other = (QueryId) o;
      return this.finalId.equals(other.finalId);
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return finalId.hashCode();
  }

  @Override
  public final int compareTo(final QueryId o) {
    return this.finalId.compareTo(o.finalId);
  }
}