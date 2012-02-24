/**
 * 
 */
package nta.engine.planner.logical;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

import nta.engine.parser.QueryBlock.Target;

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
  public void accept(LogicalNodeVisitor visitor) {
    // nothing
  }
}
