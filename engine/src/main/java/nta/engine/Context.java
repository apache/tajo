package nta.engine;

import nta.catalog.TableDesc;
import nta.engine.parser.CreateTableStmt;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.parser.StatementType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 이 클래스는 주어진 질의가 실행 중인 동안 질의에 대한 정보를 유지한다.
 * 
 * @author Hyunsik Choi
 */
public abstract class Context {
  private Map<String, String> aliasMap = new HashMap<String, String>();
  
  // Hints
  protected StatementType stmtType;
  protected boolean hasWhereClause;
  protected boolean hasGroupByClause;
  protected boolean hasJoinClause;
  private Target [] targets;
  
  public abstract TableDesc getTable(String id);
  
  public Collection<String> getInputTables() {
    return aliasMap.values();
  }

  public void renameTable(final String tableName, final String alias) {
    aliasMap.put(alias, tableName);
  }
  
  public void renameTable(final QueryBlock.FromTable table) {
    String alias = table.hasAlias() ? table.getAlias() : table.getTableName();
    aliasMap.put(alias, table.getTableName());
  }
  
  public String getActualTableName(final String aliasName) {
    return this.aliasMap.get(aliasName);
  }
  
  public void makeHints(ParseTree query) {
    QueryBlock block = null;
    switch (query.getType()) {
    case SELECT:
      block = (QueryBlock) query;      
      break;
    case CREATE_TABLE:
      CreateTableStmt createTableStmt = (CreateTableStmt) query;
      if (createTableStmt.hasSelectStmt()) {
        block = ((CreateTableStmt) query).getSelectStmt();
      }
      break;
    }
    
    stmtType = query.getType();
    if (block != null) {
      hasWhereClause = block.hasWhereClause();
      hasGroupByClause = block.hasGroupbyClause();

      if (block.hasFromClause()) {
        if (block.hasExplicitJoinClause()) {
          hasJoinClause = true;
        }
      }
      targets = block.getTargetList();
    }
  }

  public void setTargets(Target [] targets) {
    this.targets = targets;
  }
  
  public StatementType getStatementType() {
    return stmtType;
  }

  public Target[] getTargetList() {
    return targets;
  }
  
  private int i = 0;
  public String getUnnamedColumn() {    
    String unnamed = "column_" + i;
    i++;
    return unnamed;
  }
  
  public void mergeContext(Context ctx) {
    aliasMap.putAll(ctx.aliasMap);
    if (this.targets == null) {
      this.targets = ctx.targets;
    } else {
      /*
      if (!TUtil.checkEquals(this.targets, ctx.targets)) {
        throw new InvalidQueryException("target lists of queries are not matched");
      }*/
    }
  }
}