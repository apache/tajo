/**
 * 
 */
package nta.engine;

import java.util.HashMap;
import java.util.Map;

import nta.catalog.CatalogService;
import nta.catalog.TableDesc;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.parser.QueryBlock.Target;

/**
 * 실행 중인 질의에 대한 정보를 담는다.
 * 
 * @author Hyunsik Choi
 */
public class QueryContext implements Context {
  private final CatalogService catalog;
  private final Map<String, TableDesc> tableMap = new HashMap<String, TableDesc>();
  private final QueryBlock block;

  private QueryContext(CatalogService catalog, TableDesc[] tables, QueryBlock block) {
    this.catalog = catalog;
    for (TableDesc table : tables) {
      tableMap.put(table.getId(), table);
    }
    this.block = block;
  }

  public static class Factory {
    private final CatalogService catalog;

    public Factory(CatalogService catalog) {
      this.catalog = catalog;
    }

    public QueryContext create(QueryBlock query) {
      TableDesc tables[] = new TableDesc[query.getFromTables().length];
      int i = 0;
      for (FromTable from : query.getFromTables()) {
        tables[i++] =
            catalog.getTableDesc(from.getTableId());
      }

      return new QueryContext(catalog, tables, query);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see nta.engine.Context#getInputTable(java.lang.String)
   */
  @Override
  public TableDesc getInputTable(String id) {
    return tableMap.get(id);
  }

  public CatalogService getCatalog() {
    return this.catalog;
  }
  

  @Override
  public boolean hasWhereClause() {
    return block.hasWhereClause();
  }

  @Override
  public boolean hasGroupByClause() {
    return block.hasGroupbyClause();
  }
  
  @Override
  public boolean hasJoinClause() {    
    return block.getFromTables().length > 1;
  }

  @Override
  public Target[] getTargetList() {
    return block.getTargetList();
  }
}