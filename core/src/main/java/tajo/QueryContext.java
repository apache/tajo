/**
 * 
 */
package tajo;

import tajo.catalog.CatalogService;
import tajo.catalog.TableDesc;

/**
 * 실행 중인 질의에 대한 정보를 담는다.
 * 
 * @author Hyunsik Choi
 */
public class QueryContext extends Context {
  private final CatalogService catalog;

  public QueryContext(CatalogService catalog) {
    this.catalog = catalog;
  }

  public static class Factory {
    private final CatalogService catalog;

    public Factory(CatalogService catalog) {
      this.catalog = catalog;
    }
    
    public QueryContext create() {
      return new QueryContext(catalog);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see Context#getInputTable(java.lang.String)
   */
  @Override
  public TableDesc getTable(String id) {
    return catalog.getTableDesc(id);
  }
}