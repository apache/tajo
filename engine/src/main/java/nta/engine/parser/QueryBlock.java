package nta.engine.parser;

/**
 * This class contains a set of meta data about a query statement.
 * 
 * @author Hyunsik Choi
 *
 */
public class QueryBlock {
  
  private FromTable [] fromTables = null;
  private EvalTreeBin whereCond = null;
  
  public QueryBlock() {}
  
  // From Clause
  public final boolean hasFromClause() {
    return fromTables != null;
  }
  
  public final void setFromTables(FromTable [] tables) {
    this.fromTables = tables;
  }
  
  public final int getNumFromTables() {
    return fromTables.length;
  }
  
  public final FromTable [] getFromTables() {
    return this.fromTables;
  }
  
  // Where Clause
  public final void setWhereCond(EvalTreeBin whereCond) {
    this.whereCond = whereCond;
  }
  
  public final EvalTreeBin getWhereCond() {
    return this.whereCond;
  }
  
  public static class Target {
    private final String column;
    private String alias = null;
    
    public Target(final String column) {
      this.column = column;
    }
    
    public Target(final String column, String alias) {
      this(column);
      this.alias = alias;
    }
    
    public final String getAlias() {
      return alias;
    }

    public final boolean hasAlias() {
      return alias != null;
    }

    public final String toString() {
      if (alias != null)
        return column + " as " + alias;
      else
        return column;
    }
  }
  
  public static class FromTable {
    private final String tableName;
    private String alias = null;

    public FromTable(final String tableName) {
      this.tableName = tableName;
    }

    public FromTable(String tableName, String alias) {
      this(tableName);
      this.alias = alias;
    }

    public final String getTableName() {
      return this.tableName;
    }
    
    public final void setAlias(String alias) {
      this.alias = alias;
    }

    public final String getAlias() {
      return alias;
    }

    public final boolean hasAlias() {
      return alias != null;
    }

    public final String toString() {
      if (alias != null)
        return tableName + " as " + alias;
      else
        return tableName;
    }
  }
}
