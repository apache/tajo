/**
 * 
 */
package nta.engine;

import java.util.HashMap;
import java.util.Map;

import nta.catalog.CatalogService;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.Target;

import com.google.common.annotations.VisibleForTesting;


/**
 * 실행 중인 subquery에 대한 정보를 담는다. 
 * 
 * @author Hyunsik Choi
 *
 */
public class SubqueryContext extends Context {
  private final Map<String, Fragment> fragmentMap
    = new HashMap<String, Fragment>();
  
  private int queryId;
  private QueryBlock query;
  
  private SubqueryContext(int queryId, Fragment [] fragments) {
    this.queryId = queryId;
    
    for(Fragment t : fragments) {
      fragmentMap.put(t.getId(), t);
    }
  }
  
  public void setParseTree(QueryBlock query) {
    this.query = query;
  }
  
  public static class Factory {
    private final CatalogService catalog;
    public Factory(CatalogService catalog) {
      this.catalog = catalog;
    }
    
    @VisibleForTesting
    public SubqueryContext create(Fragment [] frags) {
      return new SubqueryContext(0, frags);
    }
    
    public SubqueryContext create(SubQueryRequest request) {
      return new SubqueryContext(request.getId(), 
          request.getFragments().toArray(
              new Fragment [request.getFragments().size()]));
    }
  }
  
  public int getQueryId() {
    return this.queryId;
  }

  @Override
  public Fragment getTable(String id) {
    return fragmentMap.get(id);
  }
  
  @Override
  public boolean hasWhereClause() {
    return query.hasWhereClause();
  }

  @Override
  public boolean hasGroupByClause() {
    return query.hasGroupbyClause();
  }

  @Override
  public Target[] getTargetList() {
    return query.getTargetList();
  }

  @Override
  public boolean hasJoinClause() {
    return query.getFromTables().length > 1;
  }
}