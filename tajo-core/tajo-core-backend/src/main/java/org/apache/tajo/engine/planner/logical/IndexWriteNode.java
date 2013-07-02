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

/**
 * 
 */
package org.apache.tajo.engine.planner.logical;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.engine.json.GsonCreator;
import org.apache.tajo.engine.parser.CreateIndexStmt;

public class IndexWriteNode extends UnaryNode {
  @Expose private String indexName;
  @Expose private boolean unique = false;
  @Expose private String tableName;
  @Expose private IndexMethod method = IndexMethod.TWO_LEVEL_BIN_TREE;
  @Expose private SortSpec[] sortSpecs;
  @Expose private Options params = null;

  public IndexWriteNode(CreateIndexStmt stmt) {
    super(ExprType.CREATE_INDEX);
    this.indexName = stmt.getIndexName();
    this.unique = stmt.isUnique();
    this.tableName = stmt.getTableName();
    this.method = stmt.getMethod();
    this.sortSpecs = stmt.getSortSpecs();
    this.params = stmt.hasParams() ? stmt.getParams() : null;
  }
  
  public String getIndexName() {
    return this.indexName;
  }
  
  public boolean isUnique() {
    return this.unique;
  }
  
  public void setUnique() {
    this.unique = true;
  }
  
  public String getTableName() {
    return this.tableName;
  }
  
  public IndexMethod getMethod() {
    return this.method;
  }
  
  public SortSpec[] getSortSpecs() {
    return this.sortSpecs;
  }
  
  public boolean hasParams() {
    return this.params != null;
  }
  
  public Options getParams() {
    return this.params;
  }

  public String toJSON() {
    for( int i = 0 ; i < this.sortSpecs.length ; i ++ ) {
      sortSpecs[i].getSortKey().initFromProto();
    }
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
  
  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }
}
