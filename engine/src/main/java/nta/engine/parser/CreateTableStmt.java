/**
 * 
 */
package nta.engine.parser;

import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.StoreType;

import org.apache.hadoop.fs.Path;

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
  
  private CreateTableStmt(final String tableName) {
    super(StatementType.CREATE_TABLE);
    this.tableName = tableName;
  }
  
  public CreateTableStmt(final String tableName, final Schema schema, 
      StoreType storeType, final Path path) {
    this(tableName);
    this.schema = schema;
    this.storeType = storeType;
    this.path = path;
  }
  
  public CreateTableStmt(final String tableName, final QueryBlock selectStmt) {    
    this(tableName);
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
