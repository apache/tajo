/**
 * 
 */
package nta.engine.parser;

/**
 * @author Hyunsik Choi
 */
public class CreateTableStmt extends ParseTree {
  private final String tableName;
  private final QueryBlock selectStmt;
  
  public CreateTableStmt(final String tableName, final QueryBlock selectStmt) {
    super(StatementType.CREATE_TABLE);
    this.tableName = tableName;
    this.selectStmt = selectStmt;
  }
  
  public final String getTableName() {
    return this.tableName;
  }
  
  public final boolean hasSelectStmt() {
    return this.selectStmt != null;
  }
  
  public final QueryBlock getSelectStmt() {
    return this.selectStmt;
  } 
}
