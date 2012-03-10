/**
 * 
 */
package nta.engine.planner.logical;

import nta.engine.parser.QueryBlock.Target;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

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
    return null;
  }
  
  public Target [] getExprs() {
    return this.exprs;
  }
  
  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
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
