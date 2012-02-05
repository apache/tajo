/**
 * 
 */
package nta.engine.planner.logical;

import com.google.gson.annotations.Expose;

import nta.engine.json.GsonCreator;
import nta.engine.parser.QueryBlock.SortKey;

/**
 * @author Hyunsik Choi
 *
 */
public final class SortNode extends UnaryNode {
	@Expose
  private SortKey [] sortKeys;
	
	public SortNode() {
		super();
	}
  
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
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
