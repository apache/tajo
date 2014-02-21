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

import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.util.TUtil;

public class CreateTableNode extends StoreTableNode implements Cloneable {
  @Expose private Schema schema;
  @Expose private Path path;
  @Expose private boolean external;

  public CreateTableNode(int pid) {
    super(pid, NodeType.CREATE_TABLE);
  }

  public void setTableSchema(Schema schema) {
    this.schema = schema;
  }
    
  public Schema getTableSchema() {
    return this.schema;
  }

  public Schema getLogicalSchema() {
    if (hasPartition()) {
      Schema logicalSchema = new Schema(schema);
      logicalSchema.addColumns(getPartitionMethod().getExpressionSchema());
      return logicalSchema;
    } else {
      return schema;
    }
  }

  public boolean hasPath() {
    return this.path != null;
  }

  public void setPath(Path path) {
    this.path = path;
  }
  
  public Path getPath() {
    return this.path;
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

  @Override
  public PlanString getPlanString() {
    return new PlanString(this);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CreateTableNode) {
      CreateTableNode other = (CreateTableNode) obj;
      return super.equals(other)
          && this.schema.equals(other.schema)
          && this.external == other.external
          && TUtil.checkEquals(path, other.path);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    CreateTableNode store = (CreateTableNode) super.clone();
    store.tableName = tableName;
    store.schema = (Schema) schema.clone();
    store.storageType = storageType;
    store.external = external;
    store.path = path != null ? new Path(path.toString()) : null;
    store.options = (Options) (options != null ? options.clone() : null);
    return store;
  }

  public String toString() {
    return "CreateTable (table=" + tableName + ", external=" + external + ", storeType=" + storageType + ")";
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