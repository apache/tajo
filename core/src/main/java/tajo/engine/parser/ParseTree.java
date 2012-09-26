package tajo.engine.parser;

import tajo.engine.parser.QueryBlock.FromTable;
import tajo.engine.planner.PlanningContext;

import java.util.Map.Entry;

public abstract class ParseTree {
  protected final PlanningContext context;
  protected final StatementType type;
  private final TableMap tableMap = new TableMap();

  public ParseTree(final PlanningContext context, final StatementType type) {
    this.context = context;
    this.type = type;
  }

  protected void addTableRef(FromTable table) {
    tableMap.addFromTable(table);
  }

  protected void addTableRef(String tableName, String alias) {
    tableMap.addFromTable(tableName, alias);
  }

  public StatementType getType() {
    return this.type;
  }

  public String getTableNameByAlias(String alias) {
    return tableMap.getTableNameByAlias(alias);
  }

  public Iterable<String> getAllTableNames() {
    return tableMap.getAllTableNames();
  }

  public Iterable<Entry<String, String>> getAliasToNames() {
    return tableMap.getAliasToNames();
  }
}
