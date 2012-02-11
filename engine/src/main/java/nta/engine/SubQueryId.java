package nta.engine;

import java.text.NumberFormat;

/**
 * @author Hyunsik Choi
 */
public class SubQueryId implements Comparable<SubQueryId> {
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(3);
  }

  private final String finalId;
  
  
  public SubQueryId(final QueryId queryId, final int id) {
    this.finalId = queryId.toString() + QueryId.SEPERATOR 
        + idFormat.format(id);
  }
  
  public final String toString() {
    return this.finalId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof SubQueryId) {
      SubQueryId other = (SubQueryId) o;
      return this.finalId.equals(other.finalId);
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return finalId.hashCode();
  }

  @Override
  public final int compareTo(final SubQueryId o) {
    return this.finalId.compareTo(o.finalId);
  }
}
