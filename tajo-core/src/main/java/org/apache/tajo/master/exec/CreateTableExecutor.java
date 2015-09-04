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

package org.apache.tajo.master.exec;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.*;
import org.apache.tajo.exception.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.plan.logical.CreateTableNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.Pair;

import java.io.IOException;
import java.net.URI;

/**
 * An executor for Create Table command in QueryCoordinator
 */
public class CreateTableExecutor {
  private static final Log LOG = LogFactory.getLog(DDLExecutor.class);

  private final CatalogService catalog;

  public CreateTableExecutor(TajoMaster.MasterContext context) {
    this.catalog = context.getCatalog();
  }

  public TableDesc create(QueryContext queryContext, CreateTableNode createTable, boolean ifNotExists)
      throws IOException, TajoException {

    TableMeta meta;
    if (createTable.hasOptions()) {
      meta = CatalogUtil.newTableMeta(createTable.getStorageType(), createTable.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(createTable.getStorageType());
    }

    if(PlannerUtil.isFileStorageType(createTable.getStorageType()) && createTable.isExternal()){
      Preconditions.checkState(createTable.hasUri(), "ERROR: LOCATION must be given.");
    }

    return create(
        queryContext,
        createTable.getTableName(),
        createTable.getTableSpaceName(),
        createTable.getTableSchema(),
        meta,
        createTable.getUri(),
        createTable.isExternal(),
        createTable.getPartitionMethod(),
        ifNotExists);
  }

  public TableDesc create(QueryContext queryContext,
                          String tableName,
                          @Nullable String tableSpaceName,
                          Schema schema,
                          TableMeta meta,
                          @Nullable URI uri,
                          boolean isExternal,
                          @Nullable PartitionMethodDesc partitionDesc,
                          boolean ifNotExists) throws IOException, TajoException {

    Pair<String, String> separatedNames = getQualifiedName(queryContext.getCurrentDatabase(), tableName);
    String databaseName = separatedNames.getFirst();
    String simpleTableName = separatedNames.getSecond();
    String qualifiedName = CatalogUtil.buildFQName(databaseName, simpleTableName);

    // Check if the table to be created already exists
    boolean exists = catalog.existsTable(databaseName, simpleTableName);
    if (exists) {
      return handlExistence(ifNotExists, qualifiedName);
    }

    Tablespace tableSpace = getTablespaceHandler(tableSpaceName, uri);

    TableDesc desc;
    URI tableUri = isExternal ? uri : tableSpace.getTableUri(databaseName, simpleTableName);
    desc = new TableDesc(qualifiedName, schema, meta, tableUri, isExternal);

    if (partitionDesc != null) {
      desc.setPartitionMethod(partitionDesc);
    }

    tableSpace.createTable(desc, ifNotExists);
    catalog.createTable(desc);
    LOG.info("relation '" + qualifiedName + "' created.");
    return desc;
  }

  private TableDesc handlExistence(boolean ifNotExists, String qualifiedName)
      throws DuplicateTableException, UndefinedTableException {

    if (ifNotExists) {
      LOG.info("relation \"" + qualifiedName + "\" is already exists.");
      return catalog.getTableDesc(qualifiedName);
    } else {
      throw new DuplicateTableException(qualifiedName);
    }
  }

  private Pair<String, String> getQualifiedName(String currentDatabase, String tableName) {
    if (CatalogUtil.isFQTableName(tableName)) {
      String [] splitted = CatalogUtil.splitFQTableName(tableName);
      return new Pair<>(splitted[0], splitted[1]);
    } else {
      return new Pair<>(currentDatabase, tableName);
    }
  }

  private Tablespace getTablespaceHandler(@Nullable String tableSpaceName, @Nullable URI tableUri)
      throws UndefinedTablespaceException {

    if (tableSpaceName != null) {
      Optional<Tablespace> ts = (Optional<Tablespace>) TablespaceManager.getByName(tableSpaceName);
      if (ts.isPresent()) {
        return ts.get();
      } else {
        throw new UndefinedTablespaceException(tableSpaceName);
      }
    } else if (tableUri != null) {
      Optional<Tablespace> ts = TablespaceManager.get(tableUri);
      if (ts.isPresent()) {
        return ts.get();
      } else {
        throw new UndefinedTablespaceException(tableUri.toString());
      }
    } else {
      return TablespaceManager.getDefault();
    }
  }
}
