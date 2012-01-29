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


/**
 * 실행 중인 subquery에 대한 정보를 담는다. 
 * 
 * @author Hyunsik Choi
 *
 */
public class SubqueryContext extends Context {
  private final CatalogService catalog;
  private final Map<String, Fragment> fragmentMap
    = new HashMap<String, Fragment>();
  private QueryBlock query;
  
  private SubqueryContext(CatalogService catalog, Fragment [] fragments) {
    this.catalog = catalog;
    
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
    
    public SubqueryContext create(Fragment [] fragments) {
      return new SubqueryContext(catalog, fragments);
    }
    
    public SubqueryContext create(SubQueryRequest request) {
      return new SubqueryContext(catalog, request.getFragments().
          toArray(new Fragment [request.getFragments().size()]));
    }
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