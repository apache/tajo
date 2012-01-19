/**
 * 
 */
package nta.engine.planner.logical;

import nta.catalog.Schema;
import nta.engine.parser.QueryBlock.SortKey;

/**
 * @author Hyunsik Choi
 *
 */
public final class SortNode extends UnaryNode {
  private final SortKey [] sortKeys;
  
  /**
   * @param opType
   */
  public SortNode(SortKey [] sortKeys) {
    super(ExprType.SORT);
    this.sortKeys = sortKeys;
  }
  
  public SortKey [] getSortKeys() {
    return this.sortKeys;
  }

  /* (non-Javadoc)
   * @see nta.engina.planner.logical.LogicalNode#getSchema()
   */
  @Override
  public Schema getOutputSchema() {
    return getSubNode().getOutputSchema();
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("Order By ");
    for (int i = 0; i < sortKeys.length; i++) {    
      sb.append(sortKeys[i].getSortKey().getName()+" "+
          (sortKeys[i].isAscending() ? "asc" : "desc"));
      if(i < sortKeys.length - 1) {
        sb.append(",");
      }
    }
    return sb.toString()+"\n"
        + getSubNode().toString();
  }
}
