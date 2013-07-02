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
package org.apache.tajo.engine.parser;

import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.engine.planner.PlanningContext;

public class CreateIndexStmt extends ParseTree {
  private String idxName;
  private boolean unique = false;
  private String tableName;
  private IndexMethod method = IndexMethod.TWO_LEVEL_BIN_TREE;
  private SortSpec[] sortSpecs;
  private Options params = null;

  public CreateIndexStmt(final PlanningContext context) {
    super(context, StatementType.CREATE_INDEX);
  }

  public CreateIndexStmt(final PlanningContext context, String idxName,
                         boolean unique, String tableName,
                         SortSpec [] sortSpecs) {
    this(context);
    this.idxName = idxName;
    this.unique = unique;
    this.tableName = tableName;
    this.sortSpecs = sortSpecs;
  }
  
  public void setIndexName(String name) {
    this.idxName = name;
  }
  
  public String getIndexName() {
    return this.idxName;
  }
  
  public boolean isUnique() {
    return this.unique;
  }
  
  public void setUnique() {
    this.unique = true;
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
    addTableRef(tableName, tableName);
  }
  
  public String getTableName() {
    return this.tableName;
  }
  
  public void setMethod(IndexMethod method) {
    this.method = method;
  }
  
  public IndexMethod getMethod() {
    return this.method;
  }
  
  public void setSortSpecs(SortSpec[] sortSpecs) {
    Preconditions.checkNotNull(sortSpecs);
    Preconditions.checkArgument(sortSpecs.length > 0,
        "Sort specifiers must be at least one");
    this.sortSpecs = sortSpecs;
  }
  
  public SortSpec[] getSortSpecs() {
    return this.sortSpecs;
  }
  
  public boolean hasParams() {
    return this.params != null;
  }
  
  public void setParams(Options params) {
    this.params = params;
  }
  
  public Options getParams() {
    return this.params;
  }
}