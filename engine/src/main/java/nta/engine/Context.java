package nta.engine;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import nta.catalog.TableDesc;
import nta.engine.parser.CreateTableStmt;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.parser.StatementType;

/**
 * 이 클래스는 주어진 질의가 실행 중인 동안 질의에 대한 정보를 유지한다.
 * 
 * @author Hyunsik Choi
 */
public abstract class Context {
  private Map<String, String> aliasMap = new HashMap<String, String>();
  
  // Hints
  protected boolean hasWhereClause;
  protected boolean hasGroupByClause;
  protected boolean hasJoinClause;
  private Target [] targets;
  
  public abstract TableDesc getTable(String id);
  
  public Collection<String> getInputTables() {
    return aliasMap.values();
  }
  
  public void renameTable(final String tableName, final String aliasName) {
    aliasMap.put(aliasName, tableName);
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
      block = ((CreateTableStmt) query).getSelectStmt();
      break;
    }
    
    if (query.getType() == StatementType.SELECT
        || query.getType() == StatementType.CREATE_TABLE) {
      hasWhereClause = block.hasWhereClause();
      hasGroupByClause = block.hasGroupbyClause();
      hasJoinClause = block.hasFromClause() ? block.getFromTables().length > 1
          : false;
      targets = block.getTargetList();
    }
  }

  public Target[] getTargetList() {
    return targets;
  }
}