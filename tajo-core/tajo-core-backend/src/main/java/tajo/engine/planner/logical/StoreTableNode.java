package tajo.engine.planner.logical;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import tajo.catalog.Column;
import tajo.catalog.Options;
import tajo.master.SubQuery;
import tajo.util.TUtil;

import static tajo.catalog.proto.CatalogProtos.StoreType;

/**
 * @author Hyunsik Choi
 * 
 */
public class StoreTableNode extends UnaryNode implements Cloneable {
  @Expose private String tableName;
  @Expose private StoreType storageType = StoreType.CSV;
  @Expose private SubQuery.PARTITION_TYPE partitionType;
  @Expose private int numPartitions;
  @Expose private Column [] partitionKeys;
  @Expose private boolean local;
  @Expose private Options options;

  public StoreTableNode(String tableName) {
    super(ExprType.STORE);
    this.tableName = tableName;
    this.local = false;
  }

  public final String getTableName() {
    return this.tableName;
  }

  public final void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setStorageType(StoreType storageType) {
    this.storageType = storageType;
  }

  public StoreType getStorageType() {
    return this.storageType;
  }

  public final void setLocal(boolean local) {
    this.local = local;
  }

  public final boolean isLocal() {
    return this.local;
  }
    
  public final int getNumPartitions() {
    return this.numPartitions;
  }
  
  public final boolean hasPartitionKey() {
    return this.partitionKeys != null;
  }
  
  public final Column [] getPartitionKeys() {
    return this.partitionKeys;
  }

  public final void setListPartition() {
    this.partitionType = SubQuery.PARTITION_TYPE.LIST;
    this.partitionKeys = null;
    this.numPartitions = 0;
  }
  
  public final void setPartitions(SubQuery.PARTITION_TYPE type, Column [] keys, int numPartitions) {
    Preconditions.checkArgument(keys.length >= 0, 
        "At least one partition key must be specified.");
    Preconditions.checkArgument(numPartitions > 0,
        "The number of partitions must be positive: %s", numPartitions);

    this.partitionType = type;
    this.partitionKeys = keys;
    this.numPartitions = numPartitions;
  }

  public SubQuery.PARTITION_TYPE getPartitionType() {
    return this.partitionType;
  }

  public boolean hasOptions() {
    return this.options != null;
  }

  public void setOptions(Options options) {
    this.options = options;
  }

  public Options getOptions() {
    return this.options;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StoreTableNode) {
      StoreTableNode other = (StoreTableNode) obj;
      return super.equals(other)
          && this.tableName.equals(other.tableName)
          && this.storageType.equals(other.storageType)
          && this.numPartitions == other.numPartitions
          && TUtil.checkEquals(partitionKeys, other.partitionKeys)
          && TUtil.checkEquals(options, other.options)
          && subExpr.equals(other.subExpr);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    StoreTableNode store = (StoreTableNode) super.clone();
    store.tableName = tableName;
    store.storageType = storageType != null ? storageType : null;
    store.numPartitions = numPartitions;
    store.partitionKeys = partitionKeys != null ? partitionKeys.clone() : null;
    store.options = options != null ? (Options) options.clone() : null;
    return store;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"Store\": {\"table\": \""+tableName);
    if (storageType != null) {
      sb.append(", storage: "+ storageType.name());
    }
    sb.append(", partnum: ").append(numPartitions).append("}")
    .append(", ");
    if (partitionKeys != null) {
      sb.append("\"partition keys: [");
      for (int i = 0; i < partitionKeys.length; i++) {
        sb.append(partitionKeys[i]);
        if (i < partitionKeys.length - 1)
          sb.append(",");
      }
      sb.append("],");
    }
    
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",")
    .append("\n  \"in schema\": ").append(getInSchema())
    .append("}");
    
    return sb.toString() + "\n"
        + getSubNode().toString();
  }
}