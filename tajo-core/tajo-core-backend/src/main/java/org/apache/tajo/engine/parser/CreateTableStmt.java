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

package org.apache.tajo.engine.parser;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.engine.planner.PlanningContext;
import org.apache.tajo.engine.planner.PlanningContextImpl;

public class CreateTableStmt extends ParseTree {
  private final String tableName;
  private Schema schema;  
  private StoreType storeType;
  private Path path;
  private QueryBlock selectStmt;
  private Options options;
  private boolean external;

  public CreateTableStmt(final PlanningContext context,
                         final String tableName) {
    super(context, StatementType.CREATE_TABLE);
    this.tableName = tableName;
  }

  public CreateTableStmt(final PlanningContext context,
                         final String tableName, final Schema schema,
                         StoreType storeType) {
    super(context, StatementType.CREATE_TABLE);
    addTableRef(tableName, tableName);
    this.tableName = tableName;
    this.schema = schema;
    this.storeType = storeType;
  }

  public CreateTableStmt(final PlanningContextImpl context,
                         final String tableName, final QueryBlock selectStmt) {
    super(context, StatementType.CREATE_TABLE_AS);
    context.setOutputTableName(tableName);
    addTableRef(tableName, tableName);
    this.tableName = tableName;
    this.selectStmt = selectStmt;
  }
  
  public final String getTableName() {
    return this.tableName;
  }
  
  public final boolean hasQueryBlock() {
    return this.selectStmt != null;
  }
  
  public final QueryBlock getSelectStmt() {
    return this.selectStmt;
  }
  
  public final boolean hasDefinition() {
    return this.schema != null;
  }

  public boolean hasTableDef() {
    return this.schema != null;
  }

  public void setTableDef(Schema schema) {
    this.schema = schema;
  }
  
  public final Schema getTableDef() {
    return this.schema;
  }

  public boolean hasStoreType() {
    return this.storeType != null;
  }

  public void setStoreType(StoreType type) {
    this.storeType = type;
  }
  
  public final StoreType getStoreType() {
    return this.storeType;
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


  public boolean hasPath() {
    return this.path != null;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public final Path getPath() {
    return this.path;
  }

  public boolean isExternal() {
    return external;
  }

  public void setExternal(boolean external) {
    this.external = external;
  }
}
