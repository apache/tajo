package tajo.engine.parser;

/**
 * @author Hyunsik Choi
 */
public class SetStmt extends ParseTree {
  private ParseTree leftTree;
  private ParseTree rightTree;
  private boolean distinct = true;
  
  public SetStmt(StatementType type, ParseTree leftTree, ParseTree rightTree, boolean distinct) {
    super(type);
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
