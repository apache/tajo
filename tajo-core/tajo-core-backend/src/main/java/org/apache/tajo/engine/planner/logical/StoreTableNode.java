/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.logical;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import static org.apache.tajo.ipc.TajoWorkerProtocol.PartitionType;
import static org.apache.tajo.ipc.TajoWorkerProtocol.PartitionType.LIST_PARTITION;

public class StoreTableNode extends UnaryNode implements Cloneable {
  @Expose private String tableName;
  @Expose private StoreType storageType = StoreType.CSV;
  @Expose private PartitionType partitionType;
  @Expose private int numPartitions;
  @Expose private Column [] partitionKeys;
  @Expose private Options options;
  @Expose private boolean isCreatedTable = false;
  @Expose private boolean isOverwritten = false;
  @Expose private PartitionDesc partitionDesc;

  public StoreTableNode(int pid, String tableName) {
    super(pid, NodeType.STORE);
    this.tableName = tableName;
  }

  public StoreTableNode(int pid, String tableName, PartitionDesc partitionDesc) {
    super(pid, NodeType.STORE);
    this.tableName = tableName;
    this.partitionDesc = partitionDesc;
  }

  public final String getTableName() {
    return this.tableName;
  }

  public void setStorageType(StoreType storageType) {
    this.storageType = storageType;
  }

  public StoreType getStorageType() {
    return this.storageType;
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

  public final void setDefaultParition() {
    this.partitionType = LIST_PARTITION;
    this.partitionKeys = null;
    this.numPartitions = 1;
  }
  
  public final void setPartitions(PartitionType type, Column [] keys, int numPartitions) {
    Preconditions.checkArgument(keys.length >= 0, 
        "At least one partition key must be specified.");
    Preconditions.checkArgument(numPartitions > 0,
        "The number of partitions must be positive: %s", numPartitions);

    this.partitionType = type;
    this.partitionKeys = keys;
    this.numPartitions = numPartitions;
  }

  public PartitionType getPartitionType() {
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

  public PartitionDesc getPartitions() {
    return partitionDesc;
  }

  public void setPartitions(PartitionDesc partitionDesc) {
    this.partitionDesc = partitionDesc;
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString("Store");
    planStr.appendTitle(" into ").appendTitle(tableName);
    planStr.addExplan("Store type: " + storageType);

    return planStr;
  }

  public boolean isCreatedTable() {
    return isCreatedTable;
  }

  public void setCreateTable() {
    isCreatedTable = true;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StoreTableNode) {
      StoreTableNode other = (StoreTableNode) obj;
      boolean eq = super.equals(other);
      eq = eq && this.tableName.equals(other.tableName);
      eq = eq && this.storageType.equals(other.storageType);
      eq = eq && this.numPartitions == other.numPartitions;
      eq = eq && TUtil.checkEquals(partitionKeys, other.partitionKeys);
      eq = eq && TUtil.checkEquals(options, other.options);
      eq = eq && isCreatedTable == other.isCreatedTable;
      eq = eq && isOverwritten == other.isOverwritten;
      eq = eq && TUtil.checkEquals(partitionDesc, other.partitionDesc);
      return eq;
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
    store.isCreatedTable = isCreatedTable;
    store.isOverwritten = isOverwritten;
    store.partitionDesc = partitionDesc;
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
    .append("\n  \"in schema\": ").append(getInSchema());

    if(partitionDesc != null) {
      sb.append(partitionDesc.toString());
    }

    sb.append("}");
    
    if (child != null) {
      return sb.toString() + "\n"
        + getChild().toString();
    } else {
      return sb.toString();
    }
  }
}