/**
 * 
 */
package tajo.engine.parser;

import org.apache.hadoop.fs.Path;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.engine.planner.PlanningContext;

/**
 * @author Hyunsik Choi
 */
public class CreateTableStmt extends ParseTree {
  private final String tableName;
  private Schema schema;  
  private StoreType storeType;
  private Path path;
  private QueryBlock selectStmt;
  private Options options;

  private CreateTableStmt(final PlanningContext context,
                          final String tableName) {
    super(context, StatementType.CREATE_TABLE);
    addTableRef(tableName, tableName);
    this.tableName = tableName;
  }

  public CreateTableStmt(final PlanningContext context,
                         final String tableName, final Schema schema,
                         StoreType storeType, final Path path) {
    this(context, tableName);
    this.schema = schema;
    this.storeType = storeType;
    this.path = path;
  }

  public CreateTableStmt(final PlanningContext context,
                         final String tableName, final QueryBlock selectStmt) {
    this(context, tableName);
    this.selectStmt = selectStmt;
  }
  
  public final String getTableName() {
    return this.tableName;
  }
  
  public final boolean hasSelectStmt() {
    return this.selectStmt != null;
  }
  
  public final QueryBlock getSelectStmt() {
    return this.selectStmt;
  }
  
  public final boolean hasDefinition() {
    return this.schema != null && this.storeType != null
        && this.path != null;
  }
  
  public final Schema getSchema() {
    return this.schema;
  }
  
  public final StoreType getStoreType() {
    return this.storeType;
  }
  
  public final Path getPath() {
    return this.path;
  }
  
  public boolean hasOptions() {
    return this.options != null;
  }
  
  public void setOptions(Options opt) {
    this.options = opt;
  }
  
  public Options getOptions() {
    return this.options;
  }
}
