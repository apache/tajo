package nta.engine.planner.logical;

/**
 * @author Hyunsik Choi
 * 
 */
public class StoreTableNode extends UnaryNode {
  private String tableName;

  public StoreTableNode(String tableName) {
    super(ExprType.STORE);
  }

  public String getTableName() {
    return this.tableName;
  }
}
