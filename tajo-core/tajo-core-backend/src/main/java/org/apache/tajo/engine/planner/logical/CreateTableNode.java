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
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.util.TUtil;

public class CreateTableNode extends LogicalNode implements Cloneable {
  @Expose private String tableName;
  @Expose private Column[] partitionKeys;
  @Expose private StoreType storageType;
  @Expose private Schema schema;
  @Expose private Path path;
  @Expose private Options options;
  @Expose private boolean external;

  public CreateTableNode(int pid, String tableName, Schema schema) {
    super(pid, NodeType.CREATE_TABLE);
    this.tableName = tableName;
    this.schema = schema;
  }

  public final String getTableName() {
    return this.tableName;
  }
    
  public Schema getSchema() {
    return this.schema;
  }

  public void setStorageType(StoreType storageType) {
    this.storageType = storageType;
  }

  public StoreType getStorageType() {
    return this.storageType;
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
  
  public boolean hasOptions() {
    return this.options != null;
  }
  
  public void setOptions(Options opt) {
    this.options = opt;
  }
  
  public Options getOptions() {
    return this.options;
  }

  public boolean isExternal() {
    return external;
  }

  public void setExternal(boolean external) {
    this.external = external;
  }


  @Override
  public PlanString getPlanString() {
    return new PlanString("CreateTable");
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CreateTableNode) {
      CreateTableNode other = (CreateTableNode) obj;
      return super.equals(other)
          && this.tableName.equals(other.tableName)
          && this.schema.equals(other.schema)
          && this.storageType == other.storageType
          && this.external == other.external
          && TUtil.checkEquals(path, other.path)
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
    store.storageType = storageType;
    store.external = external;
    store.path = path != null ? new Path(path.toString()) : null;
    store.partitionKeys = partitionKeys != null ? partitionKeys.clone() : null;
    store.options = (Options) (options != null ? options.clone() : null);
    return store;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"CreateTable\": {\"table\": \""+tableName+"\",");
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
    sb.append(",\"storeType\": \"" + this.storageType);
    sb.append(",\"path\" : \"" + this.path).append("\",");
    sb.append(",\"external\" : \"" + this.external).append("\",");
    
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",")
    .append("\n  \"in schema\": ").append(getInSchema())
    .append("}");
    
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
}