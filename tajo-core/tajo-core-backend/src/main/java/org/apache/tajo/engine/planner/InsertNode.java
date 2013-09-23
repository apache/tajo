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

package org.apache.tajo.engine.planner;

import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalNodeVisitor;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.util.TUtil;

public class InsertNode extends LogicalNode implements Cloneable {
  @Expose private boolean overwrite;
  @Expose private TableDesc targetTableDesc;
  @Expose private Schema targetSchema;
  @Expose private Path path;
  @Expose private StoreType storageType;
  @Expose private Options options;
  @Expose private LogicalNode subQuery;


  public InsertNode(int pid, TableDesc desc, LogicalNode subQuery) {
    super(pid, NodeType.INSERT);
    this.targetTableDesc = desc;
    this.subQuery = subQuery;
    this.setInSchema(subQuery.getOutSchema());
    this.setOutSchema(subQuery.getOutSchema());
  }

  public InsertNode(int pid, Path location, LogicalNode subQuery) {
    super(pid, NodeType.INSERT);
    this.path = location;
    this.subQuery = subQuery;
    this.setInSchema(subQuery.getOutSchema());
    this.setOutSchema(subQuery.getOutSchema());
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public boolean hasTargetTable() {
    return this.targetTableDesc != null;
  }

  public TableDesc getTargetTable() {
    return this.targetTableDesc;
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

  public boolean hasPath() {
    return this.path != null;
  }

  public void setPath(Path path) {
    this.path = path;
  }
  
  public Path getPath() {
    return this.path;
  }

  public boolean hasStorageType() {
    return this.storageType != null;
  }

  public void setStorageType(StoreType storageType) {
    this.storageType = storageType;
  }

  public StoreType getStorageType() {
    return this.storageType;
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

  public LogicalNode getSubQuery() {
    return this.subQuery;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof InsertNode) {
      InsertNode other = (InsertNode) obj;
      return super.equals(other)
          && TUtil.checkEquals(this.targetTableDesc, other.targetTableDesc)
          && TUtil.checkEquals(path, other.path)
          && this.overwrite == other.overwrite
          && TUtil.checkEquals(this.storageType, other.storageType)
          && TUtil.checkEquals(options, other.options)
          && subQuery.equals(other.subQuery);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    InsertNode store = (InsertNode) super.clone();
    store.targetTableDesc = targetTableDesc != null ? targetTableDesc : null;
    store.path = path != null ? new Path(path.toString()) : null;
    store.overwrite = overwrite;
    store.storageType = storageType != null ? storageType : null;
    store.options = (Options) (options != null ? options.clone() : null);
    store.subQuery = (LogicalNode) subQuery.clone();
    return store;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT ");
    if (overwrite) {
      sb.append("OVERWRITE ");
    }
    sb.append("INTO");

    if (hasTargetTable()) {
      sb.append(targetTableDesc);
    }

    if (hasPath()) {
      sb.append(" LOCATION ");
      sb.append(path);
    }

    sb.append(" [SUBQUERY]");
    
    return sb.toString();
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);    
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);    
  }

  @Override
  public PlanString getPlanString() {
    PlanString planString = new PlanString("INSERT");
    planString.addExplan(" INTO ");
    if (hasTargetTable()) {
      planString.addExplan(getTargetTable().getName());
    } else {
      planString.addExplan("LOCATION " + path);
    }
    return planString;
  }
}