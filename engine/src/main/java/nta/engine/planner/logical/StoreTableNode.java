package nta.engine.planner.logical;

import nta.catalog.Column;
import nta.engine.json.GsonCreator;
import nta.engine.utils.TUtil;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 * 
 */
public class StoreTableNode extends UnaryNode implements Cloneable {
  @Expose private String tableName;
  @Expose private int numPartitions;
  @Expose private Column [] partitionKeys;
  @Expose private boolean local;

  public StoreTableNode(String tableName) {
    super(ExprType.STORE);
    this.tableName = tableName;
    this.local = false;
  }

  public final String getTableName() {
    return this.tableName;
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
  
  public final void setLocal(boolean local) {
    this.local = local;
  }
  
  public final boolean isLocal() {
    return this.local;
  }
  
  public final void setPartitions(Column [] keys, int numPartitions) {
    Preconditions.checkArgument(keys.length >= 0, 
        "At least one partition key must be specified.");
    Preconditions.checkArgument(numPartitions > 0,
        "The number of partitions must be positive: %s", numPartitions);
    
    this.partitionKeys = keys;
    this.numPartitions = numPartitions;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StoreTableNode) {
      StoreTableNode other = (StoreTableNode) obj;
      return super.equals(other)
          && this.tableName.equals(other.tableName)
          && this.numPartitions == other.numPartitions
          && TUtil.checkEquals(partitionKeys, other.partitionKeys)
          && subExpr.equals(other.subExpr);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    StoreTableNode store = (StoreTableNode) super.clone();
    store.tableName = tableName;
    store.numPartitions = numPartitions;
    store.partitionKeys = partitionKeys != null ? partitionKeys.clone() : null;
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
    }
    
    sb.append("\n  \"out schema\": ").append(getOutputSchema()).append(",")
    .append("\n  \"in schema\": ").append(getInputSchema())
    .append("}");
    
    return sb.toString() + "\n"
        + getSubNode().toString();
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}