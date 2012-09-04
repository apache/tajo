/**
 * 
 */
package tajo.engine.parser;

import com.google.common.base.Preconditions;
import tajo.catalog.Options;
import tajo.catalog.proto.CatalogProtos.IndexMethod;
import tajo.engine.parser.QueryBlock.SortSpec;
import tajo.engine.planner.PlanningContext;

/**
 * @author Hyunsik Choi
 */
public class CreateIndexStmt extends ParseTree {
  private String idxName;
  private boolean unique = false;
  private String tableName;
  private IndexMethod method = IndexMethod.TWO_LEVEL_BIN_TREE;
  private QueryBlock.SortSpec[] sortSpecs;
  private Options params = null;

  public CreateIndexStmt(final PlanningContext context) {
    super(context, StatementType.CREATE_INDEX);
  }

  public CreateIndexStmt(final PlanningContext context, String idxName,
                         boolean unique, String tableName,
                         SortSpec [] sortSpecs) {
    this(context);
    this.idxName = idxName;
    this.unique = unique;
    this.tableName = tableName;
    this.sortSpecs = sortSpecs;
  }
  
  public void setIndexName(String name) {
    this.idxName = name;
  }
  
  public String getIndexName() {
    return this.idxName;
  }
  
  public boolean isUnique() {
    return this.unique;
  }
  
  public void setUnique() {
    this.unique = true;
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
    addTableRef(tableName, tableName);
  }
  
  public String getTableName() {
    return this.tableName;
  }
  
  public void setMethod(IndexMethod method) {
    this.method = method;
  }
  
  public IndexMethod getMethod() {
    return this.method;
  }
  
  public void setSortSpecs(QueryBlock.SortSpec[] sortSpecs) {
    Preconditions.checkNotNull(sortSpecs);
    Preconditions.checkArgument(sortSpecs.length > 1, 
        "Sort specifiers must be at least one");
    this.sortSpecs = sortSpecs;
  }
  
  public QueryBlock.SortSpec[] getSortSpecs() {
    return this.sortSpecs;
  }
  
  public boolean hasParams() {
    return this.params != null;
  }
  
  public void setParams(Options params) {
    this.params = params;
  }
  
  public Options getParams() {
    return this.params;
  }
}