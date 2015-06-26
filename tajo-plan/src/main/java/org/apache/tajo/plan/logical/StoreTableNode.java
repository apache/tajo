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

package org.apache.tajo.plan.logical;

import com.google.gson.annotations.Expose;

import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.util.TUtil;

public class StoreTableNode extends PersistentStoreNode implements Cloneable {
  @Expose protected String tableName;
  @Expose private PartitionMethodDesc partitionDesc;

  public StoreTableNode(int pid) {
    super(pid, NodeType.STORE);
  }

  protected StoreTableNode(int pid, NodeType nodeType) {
    super(pid, nodeType);
  }

  public boolean hasTargetTable() {
    return tableName != null;
  }

  /**
   * Check if a table name is specified.
   *
   * @return FALSE if this node is used for 'INSERT INTO LOCATION'. Otherwise, it will be TRUE.
   */
  public boolean hasTableName() {
    return tableName != null;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public final String getTableName() {
    return this.tableName;
  }

  public boolean hasPartition() {
    return this.partitionDesc != null;
  }

  public PartitionMethodDesc getPartitionMethod() {
    return partitionDesc;
  }

  public void setPartitionMethod(PartitionMethodDesc partitionDesc) {
    this.partitionDesc = partitionDesc;
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this);
    planStr.appendTitle(" into ").appendTitle(tableName);
    planStr.addExplan("Store type: " + storageType);

    return planStr;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((partitionDesc == null) ? 0 : partitionDesc.hashCode());
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StoreTableNode) {
      StoreTableNode other = (StoreTableNode) obj;
      boolean eq = super.equals(other);
      eq = eq && TUtil.checkEquals(this.tableName, other.tableName);
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
    store.partitionDesc = partitionDesc != null ? (PartitionMethodDesc) partitionDesc.clone() : null;
    return store;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Store Table (table=").append(tableName);
    if (storageType != null) {
      sb.append(", storage="+ storageType);
    }
    sb.append(")");
    return sb.toString();
  }
}