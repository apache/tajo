package tajo.engine.parser;

import tajo.engine.planner.PlanningContext;

/**
 * @author Hyunsik Choi
 */
public class SetStmt extends ParseTree {
  private ParseTree leftTree;
  private ParseTree rightTree;
  private boolean distinct = true;

  public SetStmt(final PlanningContext context,
                 final StatementType type,
                 final ParseTree leftTree,
                 final ParseTree rightTree,
                 boolean distinct) {
    super(context, type);
    this.leftTree = leftTree;
    this.rightTree = rightTree;
    this.distinct = distinct;
  }
  
  public boolean isDistinct() {
    return distinct;
  }
  
  public ParseTree getLeftTree() {
    return this.leftTree;
  }
  
  public ParseTree getRightTree() {
    return this.rightTree;
  }
}
