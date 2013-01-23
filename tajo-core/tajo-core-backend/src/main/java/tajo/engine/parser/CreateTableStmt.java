/**
 * 
 */
package tajo.engine.parser;

import org.apache.hadoop.fs.Path;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.engine.planner.PlanningContext;
import tajo.engine.planner.PlanningContextImpl;

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

  public CreateTableStmt(final PlanningContext context,
                         final String tableName) {
    super(context, StatementType.CREATE_TABLE);
    this.tableName = tableName;
  }

  public CreateTableStmt(final PlanningContext context,
                         final String tableName, final Schema schema,
                         StoreType storeType) {
    super(context, StatementType.CREATE_TABLE);
    addTableRef(tableName, tableName);
    this.tableName = tableName;
    this.schema = schema;
    this.storeType = storeType;
  }

  public CreateTableStmt(final PlanningContextImpl context,
                         final String tableName, final QueryBlock selectStmt) {
    super(context, StatementType.CREATE_TABLE_AS);
    context.setOutputTableName(tableName);
    addTableRef(tableName, tableName);
    this.tableName = tableName;
    this.selectStmt = selectStmt;
  }
  
  public final String getTableName() {
    return this.tableName;
  }
  
  public final boolean hasQueryBlock() {
    return this.selectStmt != null;
  }
  
  public final QueryBlock getSelectStmt() {
    return this.selectStmt;
  }
  
  public final boolean hasDefinition() {
    return this.schema != null;
  }

  public boolean hasTableDef() {
    return this.schema != null;
  }

  public void setTableDef(Schema schema) {
    this.schema = schema;
  }
  
  public final Schema getTableDef() {
    return this.schema;
  }

  public boolean hasStoreType() {
    return this.storeType != null;
  }

  public void setStoreType(StoreType type) {
    this.storeType = type;
  }
  
  public final StoreType getStoreType() {
    return this.storeType;
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


  public boolean hasPath() {
    return this.path != null;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public final Path getPath() {
    return this.path;
  }
}
