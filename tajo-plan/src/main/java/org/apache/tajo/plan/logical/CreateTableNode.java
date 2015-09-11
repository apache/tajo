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

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.util.TUtil;

import java.net.URI;

public class CreateTableNode extends StoreTableNode implements Cloneable {
  @Expose private String tableSpaceName;
  @Expose private boolean external;
  @Expose private boolean ifNotExists;
  @Expose private boolean selfDescSchema = false;

  public CreateTableNode(int pid) {
    super(pid, NodeType.CREATE_TABLE);
  }

  @Override
  public int childNum() {
    return child == null ? 0 : 1;
  }

  public Schema getLogicalSchema() {
    if (hasPartition()) {
      Schema logicalSchema = new Schema(tableSchema);
      logicalSchema.addColumns(getPartitionMethod().getExpressionSchema());
      return logicalSchema;
    } else {
      return tableSchema;
    }
  }

  public boolean hasTableSpaceName() {
    return tableSpaceName != null;
  }

  public String getTableSpaceName() {
    return tableSpaceName;
  }

  public void setTableSpaceName(String tableSpaceName) {
    this.tableSpaceName = tableSpaceName;
  }

  public boolean isExternal() {
    return external;
  }

  public void setExternal(boolean external) {
    this.external = external;
  }

  public boolean hasSubQuery() {
    return child != null;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public void setSelfDescSchema(boolean selfDescSchema) {
    this.selfDescSchema = selfDescSchema;
  }

  public boolean hasSelfDescSchema() {
    return selfDescSchema;
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this);
  }

  public int hashCode() {
    return super.hashCode() ^ Objects.hashCode(tableSchema, uri, external, ifNotExists) * 31;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CreateTableNode) {
      CreateTableNode other = (CreateTableNode) obj;
      boolean eq = super.equals(other);
      eq &= TUtil.checkEquals(tableSpaceName, other.tableSpaceName);
      eq &= this.external == other.external;
      eq &= ifNotExists == other.ifNotExists;;
      return eq;
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    CreateTableNode createTableNode = (CreateTableNode) super.clone();
    createTableNode.tableSpaceName = tableSpaceName;
    createTableNode.external = external;
    createTableNode.ifNotExists = ifNotExists;
    return createTableNode;
  }

  public String toString() {
    return "CreateTable (table=" + tableName + ", external=" + external + ", storeType=" + storageType +
        ", ifNotExists=" + ifNotExists +")";
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    if (hasSubQuery()) {
      child.preOrder(visitor);
    }
    visitor.visit(this);
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
    if (hasSubQuery()) {
      child.preOrder(visitor);
    }
  }
}