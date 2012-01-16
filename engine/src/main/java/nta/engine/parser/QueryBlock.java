package nta.engine.parser;

/**
 * This class contains a set of meta data about a query statement.
 * 
 * @author Hyunsik Choi
 *
 */
public class QueryBlock {
  
  private boolean projectAll = false;
  private boolean distinct = false;
  private Target [] targetList = null;
  private FromTable [] fromTables = null;
  private EvalTreeBin whereCond = null;
  String [] groupFields = null;
  private EvalTreeBin havingCond = null;
  
  public QueryBlock() {
    // nothing
  }
  
  public final void setProjectAll() {
    this.projectAll = true;
  }
  
  public final boolean getProjectAll() {
    return this.projectAll;
  }
  
  public final void setDistinct() {
    this.distinct = true;
  }
  
  public final boolean getDistinct() {
    return this.distinct;
  }
  
  public final void setTargetList(Target [] targets) {
    this.targetList = targets;
  }
  
  public final Target [] getTargetList() {
    return this.targetList;
  }
  
  public final void setGroupingFields(final String [] groupFields) {
    this.groupFields = groupFields;
  }
  
  public final String [] getGroupFields() {
    return this.groupFields;
  }
  
  public final void setHavingCond(final EvalTreeBin havingCond) {
    this.havingCond = havingCond;
  }
  
  public final EvalTreeBin getHavingCond() {
    return this.havingCond;
  }
  
  // From Clause
  public final boolean hasFromClause() {
    return fromTables != null;
  }
  
  public final void setFromTables(final FromTable [] tables) {
    this.fromTables = tables;
  }
  
  public final int getNumFromTables() {
    return fromTables.length;
  }
  
  public final FromTable [] getFromTables() {
    return this.fromTables;
  }
  
  // Where Clause
  public final void setWhereCond(final EvalTreeBin whereCond) {
    this.whereCond = whereCond;
  }
  
  public final EvalTreeBin getWhereCond() {
    return this.whereCond;
  }
  
  public static class Target {
    private final EvalTreeBin evalBin;
    private String alias = null;     
    
    public Target(final EvalTreeBin evalBin) {
      this.evalBin = evalBin;
    }
    
    public Target(final EvalTreeBin evalBin, final String alias) {
      this(evalBin);
      this.alias = alias;
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
