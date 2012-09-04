/**
 * 
 */
package tajo.engine.planner.logical;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock;

/**
 * @author Hyunsik Choi
 */
public class EvalExprNode extends LogicalNode {
  @Expose private QueryBlock.Target[] exprs;
  
  /**
   * 
   */
  public EvalExprNode(QueryBlock.Target[] exprs) {
    super(ExprType.EXPRS);
    this.exprs = exprs;
  }

  @Override
  public String toJSON() {
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this);
  }
  
  public QueryBlock.Target[] getExprs() {
    return this.exprs;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"EvalExpr\": {");
    sb.append("\"targets\": [");

    for (int i = 0; i < exprs.length; i++) {
      sb.append("\"").append(exprs[i]).append("\"");
      if( i < exprs.length - 1) {
        sb.append(",");
      }
    }
    sb.append("],");
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInSchema());
    sb.append("}");
    return sb.toString();
  }
  
  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    // nothing
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    // nothing
  }
}
