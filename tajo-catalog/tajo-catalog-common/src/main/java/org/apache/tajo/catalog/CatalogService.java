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

package org.apache.tajo.catalog;

import org.apache.tajo.catalog.exception.UndefinedFunctionException;
import org.apache.tajo.catalog.exception.UndefinedPartitionException;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.common.TajoDataTypes.DataType;

import java.util.Collection;
import java.util.List;

public interface CatalogService {

  /**
   *
   * @param tableSpaceName Tablespace name to be created
   * @return True if tablespace is created successfully. Otherwise, it will return FALSE.
   */
  Boolean createTablespace(String tableSpaceName, String uri);

  /**
   *
   * @param tableSpaceName Tablespace name to be created
   * @return True if tablespace is created successfully. Otherwise, it will return FALSE.
   */
  Boolean existTablespace(String tableSpaceName);

  /**
   *
   * @param tableSpaceName Tablespace name to be created
   * @return True if tablespace is created successfully. Otherwise, it will return FALSE.
   */
  Boolean dropTablespace(String tableSpaceName);

  /**
   *
   * @return All tablespace names
   */
  Collection<String> getAllTablespaceNames();
  
  /**
   * 
   */
  List<TablespaceProto> getAllTablespaces();

  /**
   *
   * @param tablespaceName Tablespace name to get
   * @return Tablespace description
   */
  TablespaceProto getTablespace(String tablespaceName);

  /**
   *
   * @param alterTablespace AlterTablespace
   * @return True if update is successfully.
   */
  Boolean alterTablespace(AlterTablespaceProto alterTablespace);

  /**
   *
   * @param databaseName Database name to be created
   * @return True if database is created successfully. Otherwise, it will return FALSE.
   */
  Boolean createDatabase(String databaseName, String tablespaceName);

  /**
   *
   * @param databaseName Database name to be dropped
   * @return True if database is dropped successfully. Otherwise, it will return FALSE.
   */
  Boolean dropDatabase(String databaseName);

  /**
   *
   * @param databaseName Database name to be checked
   * @return True if database exists. Otherwise, it will return FALSE.
   */
  Boolean existDatabase(String databaseName);

  /**
   *
   * @return All database names
   */
  Collection<String> getAllDatabaseNames();
  
  /**
   * 
   */
  List<DatabaseProto> getAllDatabases();

  /**
   * Get a table description by name
   * @param tableName table name
   * @return a table description
   * @see TableDesc
   * @throws Throwable
   */
  TableDesc getTableDesc(String databaseName, String tableName);

  /**
   * Get a table description by name
   * @return a table description
   * @see TableDesc
   * @throws Throwable
   */
  TableDesc getTableDesc(String qualifiedName);

  /**
   *
   * @return All table names which belong to a given database.
   */
  Collection<String> getAllTableNames(String databaseName);
  
  /**
   * 
   */
  List<TableDescriptorProto> getAllTables();
  
  List<TableOptionProto> getAllTableOptions();
  
  List<TableStatsProto> getAllTableStats();
  
  /**
   * 
   */
  List<ColumnProto> getAllColumns();

  List<IndexDescProto> getAllIndexes();

  /**
   *
   * @return All FunctionDescs
   */
  Collection<FunctionDesc> getFunctions();

  /**
   * Add a table via table description
   * @see TableDesc
   * @throws Throwable
   */
  boolean createTable(TableDesc desc);


  /**
   * Drop a table by name
   *
   * @param tableName table name
   * @throws Throwable
   */
  boolean dropTable(String tableName);

  boolean existsTable(String databaseName, String tableName);

  boolean existsTable(String tableName);

  PartitionMethodDesc getPartitionMethod(String databaseName, String tableName);

  boolean existPartitionMethod(String databaseName, String tableName);

  PartitionDescProto getPartition(String databaseName, String tableName, String partitionName)
      throws UndefinedPartitionException;

  List<PartitionDescProto> getPartitions(String databaseName, String tableName);

  List<TablePartitionProto> getAllPartitions();

  boolean createIndex(IndexDesc index);

  boolean existIndexByName(String databaseName, String indexName);

  boolean existIndexByColumns(String databaseName, String tableName, Column[] columns);

  boolean existIndexByColumnNames(String databaseName, String tableName, String [] columnNames);

  boolean existIndexesByTable(String databaseName, String tableName);

  IndexDesc getIndexByName(String databaseName, String indexName);

  IndexDesc getIndexByColumns(String databaseName, String tableName, Column [] columns);

  IndexDesc getIndexByColumnNames(String databaseName, String tableName, String [] columnNames);

  Collection<IndexDesc> getAllIndexesByTable(String databaseName, String tableName);

  boolean dropIndex(String databaseName, String indexName);

  boolean createFunction(FunctionDesc funcDesc);

  boolean dropFunction(String signature);

  FunctionDesc getFunction(String signature, DataType... paramTypes) throws UndefinedFunctionException;

  FunctionDesc getFunction(String signature, FunctionType funcType, DataType... paramTypes) throws UndefinedFunctionException;

  boolean containFunction(String signature, DataType... paramTypes);

  boolean containFunction(String signature, FunctionType funcType, DataType... paramTypes);

  /**
  * Add a table via table description
  * @see AlterTableDesc
  * @throws Throwable
  */
  boolean alterTable(AlterTableDesc desc);

  boolean updateTableStats(UpdateTableStatsProto stats);



}
