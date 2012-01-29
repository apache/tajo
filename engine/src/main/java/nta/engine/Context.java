package nta.engine;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import nta.catalog.TableDesc;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.Target;

/**
 * 이 클래스는 주어진 질의가 실행 중인 동안 질의에 대한 정보를 유지한다.
 * 
 * @author Hyunsik Choi
 */
public abstract class Context {
  private Map<String, String> aliasMap = new HashMap<String, String>();
  private QueryBlock query;
  
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
  
  public void setParseTree(QueryBlock query) {
    this.query = query;
  }
  
  // Hints for planning and optimization
  public boolean hasWhereClause() {
    return query.hasWhereClause();
  }

  public boolean hasGroupByClause() {
    return query.hasGroupbyClause();
  }
  
  public boolean hasJoinClause() {    
    return query.getFromTables().length > 1;
  }

  public Target[] getTargetList() {
    return query.getTargetList();
  }
}