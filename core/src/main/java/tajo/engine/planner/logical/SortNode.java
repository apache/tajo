/**
 * 
 */
package tajo.engine.planner.logical;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import tajo.catalog.Schema;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock;
import tajo.engine.utils.TUtil;

/**
 * @author Hyunsik Choi
 *
 */
public final class SortNode extends UnaryNode implements Cloneable {
	@Expose
  private QueryBlock.SortSpec[] sortKeys;
	
	public SortNode() {
		super();
	}

  public SortNode(QueryBlock.SortSpec[] sortKeys) {
    super(ExprType.SORT);
    Preconditions.checkArgument(sortKeys.length > 0, 
        "At least one sort key must be specified");
    this.sortKeys = sortKeys;
  }

  public SortNode(QueryBlock.SortSpec[] sortKeys, Schema inSchema, Schema outSchema) {
    this(sortKeys);
    this.setInSchema(inSchema);
    this.setOutSchema(outSchema);
  }
  
  public QueryBlock.SortSpec[] getSortKeys() {
    return this.sortKeys;
  }
  
  @Override 
  public boolean equals(Object obj) {
    if (obj instanceof SortNode) {
      SortNode other = (SortNode) obj;
      return super.equals(other)
          && TUtil.checkEquals(sortKeys, other.sortKeys)
          && subExpr.equals(other.subExpr);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SortNode sort = (SortNode) super.clone();
    sort.sortKeys = sortKeys.clone();
    
    return sort;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("Sort [key= ");
    for (int i = 0; i < sortKeys.length; i++) {    
      sb.append(sortKeys[i].getSortKey().getQualifiedName()).append(" ")
          .append(sortKeys[i].isAscending() ? "asc" : "desc");
      if(i < sortKeys.length - 1) {
        sb.append(",");
      }
    }
    sb.append("]");

    sb.append("\n\"out schema: " + getOutSchema()
        + "\n\"in schema: " + getInSchema());
    return sb.toString()+"\n"
        + getSubNode().toString();
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
