/**
 * 
 */
package nta.engine.planner.logical;

import com.google.gson.annotations.Expose;
import nta.engine.json.GsonCreator;
import nta.engine.parser.QueryBlock.Target;

import com.google.gson.Gson;

/**
 * @author Hyunsik Choi
 */
public class EvalExprNode extends LogicalNode {
  @Expose private Target [] exprs;
  
  /**
   * 
   */
  public EvalExprNode(Target [] exprs) {
    super(ExprType.EXPRS);
    this.exprs = exprs;
  }

  @Override
  public String toJSON() {
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this);
  }
  
  public Target [] getExprs() {
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
    sb.append("\n  \"out schema\": ").append(getOutputSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInputSchema());
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
