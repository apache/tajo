/**
 * 
 */
package nta.engine;

import java.util.HashMap;
import java.util.Map;

import nta.catalog.TableDesc;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.parser.QueryBlock.Target;


/**
 * 실행 중인 subquery에 대한 정보를 담는다. 
 * 
 * @author Hyunsik Choi
 *
 */
public class SubqueryContext implements Context {
  private final CatalogReader catalog;
  private final Map<String, Fragment> fragmentMap
    = new HashMap<String, Fragment>();
  
  private SubqueryContext(CatalogReader catalog, Fragment [] fragments) {
    this.catalog = catalog;
    
    for(Fragment t : fragments) {
      fragmentMap.put(t.getId(), t);
    }
  }
  
  public static class Factory {
    private final CatalogReader catalog;
    public Factory(CatalogReader catalog) {
      this.catalog = catalog;
    }
    
    public SubqueryContext create(Fragment [] fragments) {
      return new SubqueryContext(catalog, fragments);
    }
    
    public SubqueryContext create(SubQueryRequest request) {
      return new SubqueryContext(catalog, request.getFragments().
          toArray(new Fragment [request.getTableName().length()]));
    }
  }

  @Override
  public TableDesc getInputTable(String id) {
    return fragmentMap.get(id);
  }

  @Override
  public CatalogReader getCatalog() {
    return this.catalog;
  }
  
  @Override
  public boolean hasWhereClause() {
    // TODO - before it, SubqueryContext should be improved to
    // include some query optimization hints.
    return false;
  }

  @Override
  public boolean hasGroupByClause() {
    // TODO - before it, SubqueryContext should be improved to
    // include some query optimization hints.
    return false;  
  }

  @Override
  public Target[] getTargetList() {
    return null;
  }

  @Override
  public boolean hasJoinClause() {
    // TODO - before it, SubqueryContext should be improved to
    // include some query optimization hints.
    return false;
  }
}