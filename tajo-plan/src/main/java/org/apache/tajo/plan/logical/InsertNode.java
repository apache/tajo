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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.util.TUtil;

public class InsertNode extends StoreTableNode implements Cloneable {
  /** Overwrite or just insert */
  @Expose private boolean overwrite;
  /** a target schema of a target table */
  @Expose private Schema targetSchema;
  /** a output schema of select clause */
  @Expose private Schema projectedSchema;

  public InsertNode(int pid) {
    super(pid, NodeType.INSERT);
  }

  public void setTargetTable(TableDesc desc) {
    setTableName(desc.getName());
    if (desc.hasPartition()) {
      tableSchema = desc.getLogicalSchema();
    } else {
      tableSchema = desc.getSchema();
    }
    if (desc.getUri() != null) {
      setUri(desc.getUri());
    }
    setOptions(desc.getMeta().getOptions());
    setStorageType(desc.getMeta().getStoreType());

    if (desc.hasPartition()) {
      this.setPartitionMethod(desc.getPartitionMethod());
    }
  }

  public void setSubQuery(LogicalNode subQuery) {
    this.setChild(subQuery);
    this.setInSchema(subQuery.getOutSchema());
    this.setOutSchema(subQuery.getOutSchema());
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public boolean hasTargetSchema() {
    return this.targetSchema != null;
  }

  public Schema getTargetSchema() {
    return this.targetSchema;
  }

  public void setTargetSchema(Schema schema) {
    this.targetSchema = schema;
  }

  public boolean hasProjectedSchema() {
    return this.projectedSchema != null;
  }

  public Schema getProjectedSchema() {
    return this.projectedSchema;
  }

  public void setProjectedSchema(Schema projected) {
    this.projectedSchema = projected;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (overwrite ? 1231 : 1237);
    result = prime * result + ((uri == null) ? 0 : uri.hashCode());
    result = prime * result + ((projectedSchema == null) ? 0 : projectedSchema.hashCode());
    result = prime * result + ((tableSchema == null) ? 0 : tableSchema.hashCode());
    result = prime * result + ((targetSchema == null) ? 0 : targetSchema.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof InsertNode) {
      InsertNode other = (InsertNode) obj;
      boolean eq = super.equals(other);
      eq &= this.overwrite == other.overwrite;
      eq &= TUtil.checkEquals(this.targetSchema, other.targetSchema);
      eq &= TUtil.checkEquals(this.projectedSchema, other.projectedSchema);
      return eq;
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    InsertNode insertNode = (InsertNode) super.clone();
    insertNode.overwrite = overwrite;
    insertNode.tableSchema = new Schema(tableSchema);
    insertNode.targetSchema = targetSchema != null ? new Schema(targetSchema) : null;
    insertNode.projectedSchema = projectedSchema != null ? new Schema(projectedSchema) : null;
    insertNode.uri = uri != null ? uri : null;
    return insertNode;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("Insert (overwrite=").append(overwrite);
    if (hasTargetTable()) {
      sb.append(",table=").append(tableName);
    }
    if (hasUri()) {
      sb.append(", location=").append(uri);
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    getChild().preOrder(visitor);
    visitor.visit(this);
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
    getChild().postOrder(visitor);
  }

  @Override
  public PlanString getPlanString() {
    PlanString planString = new PlanString(this);
    planString.appendTitle(" INTO ");
    if (hasTargetTable()) {
      planString.appendTitle(getTableName());
      if (hasTargetSchema()) {
        planString.addExplan(getTargetSchema().toString());
      }
    } else {
      planString.addExplan("LOCATION " + uri);
    }
    return planString;
  }
}