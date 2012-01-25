/**
 * 
 */
package nta.engine.planner;

import java.util.HashSet;
import java.util.Stack;

import nta.catalog.ColumnBase;
import nta.engine.Context;
import nta.engine.exec.eval.EvalNode;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.UnaryNode;
import nta.engine.query.exception.InvalidQueryException;

/**
 * This class optimizes a given logical plan.
 * 
 * @author Hyunsik Choi
 *
 */
public class LogicalOptimizer {

  private LogicalOptimizer() {
  }
  
  public static LogicalNode optimize(Context ctx, LogicalNode plan) {
    if(ctx.hasWhereClause())
      pushSelection(ctx, plan);
    
    pushProjection(ctx, plan);
    
    return plan;
  }
  
  /**
   * This method pushes down the projection list into the appropriate and 
   * below logical operators.
   * @param ctx
   * @param plan
   */
  private static void pushProjection(Context ctx, LogicalNode plan) {
    HashSet<ColumnBase> targetList = new HashSet<ColumnBase>();
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    LogicalPlanner.refineInOutSchama(ctx, plan, targetList, stack);
  }
  
  /**
   * This method pushes down the selection into the appropriate and below
   * logical operators.
   * @param ctx
   * @param plan
   */
  private static void pushSelection(Context ctx, LogicalNode plan) {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    pushSelectionRecursive(ctx, plan, null, stack);
  }
  
  private static void pushSelectionRecursive(Context ctx, LogicalNode plan,
      EvalNode [] evalNode, Stack<LogicalNode> stack) {
    
    switch(plan.getType()) {
    
    case SELECTION: 
      SelectionNode selNode = (SelectionNode) plan;
      stack.push(selNode);
      pushSelectionRecursive(ctx, selNode.getSubNode(), 
          new EvalNode [] {selNode.getQual()}, stack);
      stack.pop();
      if(!stack.empty()) {
        LogicalNode node = stack.peek();
        if (node instanceof UnaryNode) {
          UnaryNode unary = (UnaryNode) node;
          unary.setSubNode(selNode.getSubNode());
        } else {
          throw new InvalidQueryException("Unexpected Logical Query Plan");
        }
      }
    case JOIN:
      break;
      
    case SCAN:
      ScanNode scanNode = (ScanNode) plan;
      scanNode.setQual(evalNode[0]);
      break;
      
    default:
      stack.push(plan);
      if (plan instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) plan;
        pushSelectionRecursive(ctx, unary.getSubNode(), evalNode, stack);
      } else if (plan instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) plan;
        pushSelectionRecursive(ctx, binary.getLeftSubNode(), evalNode, stack);
        pushSelectionRecursive(ctx, binary.getRightSubNode(), evalNode, stack);
      }
      stack.pop();
      break;
    }
  }
}
