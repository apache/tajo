package tajo.engine.planner.logical;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Column;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.engine.json.GsonCreator;
import tajo.util.TUtil;

/**
 * @author Hyunsik Choi
*/
public class CreateTableNode extends LogicalNode implements Cloneable {
  @Expose private String tableName;
  @Expose private Column[] partitionKeys;
  @Expose private StoreType storeType;
  @Expose private Schema schema;
  @Expose private Path path;
  @Expose private Options options;

  public CreateTableNode(String tableName, Schema schema, StoreType storeType, 
      Path path) {
    super(ExprType.CREATE_TABLE);
    this.tableName = tableName;
    this.schema = schema;
    this.storeType = storeType;
    this.path = path;
  }

  public final String getTableName() {
    return this.tableName;
  }
  
  public final boolean hasPartitionKey() {
    return this.partitionKeys != null;
  }
  
  public final Column [] getPartitionKeys() {
    return this.partitionKeys;
  }
  
  public final void setPartitionKeys(Column [] keys) {
    Preconditions.checkArgument(keys.length > 0, 
        "At least one partition key must be specified.");
    
    this.partitionKeys = keys;
  }
    
  public Schema getSchema() {
    return this.schema;
  }

  public StoreType getStoreType() {
    return this.storeType;
  }
  
  public Path getPath() {
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
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CreateTableNode) {
      CreateTableNode other = (CreateTableNode) obj;
      return super.equals(other)
          && this.tableName.equals(other.tableName)
          && this.schema.equals(other.schema)
          && this.storeType == other.storeType
          && this.path.equals(other.path)
          && TUtil.checkEquals(options, other.options)
          && TUtil.checkEquals(partitionKeys, other.partitionKeys);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    CreateTableNode store = (CreateTableNode) super.clone();
    store.tableName = tableName;
    store.schema = (Schema) schema.clone();
    store.storeType = storeType;
    store.path = new Path(path.toString());
    store.partitionKeys = partitionKeys != null ? partitionKeys.clone() : null;
    store.options = (Options) (options != null ? options.clone() : null);
    return store;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"Store\": {\"table\": \""+tableName+"\",");
    if (partitionKeys != null) {
      sb.append("\"partition keys: [");
      for (int i = 0; i < partitionKeys.length; i++) {
        sb.append(partitionKeys[i]);
        if (i < partitionKeys.length - 1)
          sb.append(",");
      }
      sb.append("],");
    }
    sb.append("\"schema: \"{" + this.schema).append("}");
    sb.append(",\"storeType\": \"" + this.storeType);
    sb.append(",\"path\" : \"" + this.path).append("\",");
    
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",")
    .append("\n  \"in schema\": ").append(getInSchema())
    .append("}");
    
    return sb.toString();
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);    
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);    
  }
}