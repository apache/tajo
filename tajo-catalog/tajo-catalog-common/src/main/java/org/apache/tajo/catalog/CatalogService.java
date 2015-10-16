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

import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.*;

import java.util.Collection;
import java.util.List;

public interface CatalogService {

  /**
   * @param tableSpaceName Tablespace name to be created
   */
  void createTablespace(String tableSpaceName, String uri) throws DuplicateTablespaceException;

  /**
   * @param tableSpaceName Tablespace name to be created
   */
  boolean existTablespace(String tableSpaceName);

  /**
   * @param tableSpaceName Tablespace name to be created
   */
  void dropTablespace(String tableSpaceName) throws UndefinedTablespaceException, InsufficientPrivilegeException;

  /**
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
  TablespaceProto getTablespace(String tablespaceName) throws UndefinedTablespaceException;

  /**
   * @param alterTablespace AlterTablespace
   */
  void alterTablespace(AlterTablespaceProto alterTablespace)
      throws UndefinedTablespaceException, InsufficientPrivilegeException;

  /**
   * @param databaseName Database name to be created
   */
  void createDatabase(String databaseName, String tablespaceName) throws DuplicateDatabaseException;

  /**
   * @param databaseName Database name to be dropped
   */
  void dropDatabase(String databaseName) throws UndefinedDatabaseException, InsufficientPrivilegeException;

  /**
   *
   * @param databaseName Database name to be checked
   * @return True if database exists. Otherwise, it will return FALSE.
   */
  boolean existDatabase(String databaseName);

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
   * @throws UndefinedTableException
   */
  TableDesc getTableDesc(String databaseName, String tableName) throws UndefinedTableException;

  /**
   * Get a table description by name
   * @return a table description
   * @see TableDesc
   * @throws UndefinedTableException
   */
  TableDesc getTableDesc(String qualifiedName) throws UndefinedTableException;

  /**
   *
   * @return All table names which belong to a given database.
   */
  Collection<String> getAllTableNames(String databaseName);
  
  List<TableDescriptorProto> getAllTables();
  
  List<TableOptionProto> getAllTableOptions();
  
  List<TableStatsProto> getAllTableStats();
  
  List<ColumnProto> getAllColumns();

  List<IndexDescProto> getAllIndexes();

  /**
   * @return All FunctionDescs
   */
  Collection<FunctionDesc> getFunctions();

  /**
   * Add a table via table description
   * @see TableDesc
   * @throws DuplicateTableException
   */
  void createTable(TableDesc desc) throws
      DuplicateTableException,
      InsufficientPrivilegeException,
      DuplicateDatabaseException,
      UndefinedDatabaseException;


  /**
   * Drop a table by name
   *
   * @param tableName table name
   * @throws UndefinedTableException
   * @throws InsufficientPrivilegeException
   */
  void dropTable(String tableName) throws
      UndefinedTableException,
      InsufficientPrivilegeException,
      UndefinedDatabaseException;

  boolean existsTable(String databaseName, String tableName);

  boolean existsTable(String tableName);

  PartitionMethodDesc getPartitionMethod(String databaseName, String tableName) throws
      UndefinedPartitionMethodException,
      UndefinedTableException,
      UndefinedDatabaseException;

  boolean existPartitionMethod(String databaseName, String tableName) throws UndefinedTableException,
      UndefinedDatabaseException;

  PartitionDescProto getPartition(String databaseName, String tableName, String partitionName)
      throws UndefinedPartitionException, UndefinedPartitionMethodException, UndefinedDatabaseException,
      UndefinedTableException;

  List<PartitionDescProto> getPartitions(String databaseName, String tableName);

  List<TablePartitionProto> getAllPartitions();

  void addPartitions(String databaseName, String tableName, List<PartitionDescProto> partitions
    , boolean ifNotExists) throws
      UndefinedTableException,
      DuplicatePartitionException,
      UndefinedPartitionMethodException,
      UndefinedDatabaseException;

  void createIndex(IndexDesc index) throws DuplicateIndexException, UndefinedDatabaseException, UndefinedTableException;

  boolean existIndexByName(String databaseName, String indexName);

  boolean existIndexByColumns(String databaseName, String tableName, Column[] columns);

  boolean existIndexByColumnNames(String databaseName, String tableName, String [] columnNames);

  boolean existIndexesByTable(String databaseName, String tableName);

  IndexDesc getIndexByName(String databaseName, String indexName);

  IndexDesc getIndexByColumns(String databaseName, String tableName, Column [] columns);

  IndexDesc getIndexByColumnNames(String databaseName, String tableName, String [] columnNames);

  Collection<IndexDesc> getAllIndexesByTable(String databaseName, String tableName);

  void dropIndex(String databaseName, String indexName) throws UndefinedIndexException, UndefinedDatabaseException;

  void createFunction(FunctionDesc funcDesc) throws DuplicateFunctionException;

  void dropFunction(String signature) throws UndefinedFunctionException, InsufficientPrivilegeException;

  FunctionDesc getFunction(String signature, DataType... paramTypes)
      throws AmbiguousFunctionException, UndefinedFunctionException;

  FunctionDesc getFunction(String signature, FunctionType funcType, DataType... paramTypes)
      throws AmbiguousFunctionException, UndefinedFunctionException;

  boolean containFunction(String signature, DataType... paramTypes);

  boolean containFunction(String signature, FunctionType funcType, DataType... paramTypes);

  /**
   * Add a table via table description
   *
   * @param desc
   * @throws DuplicateColumnException
   * @throws DuplicateTableException
   * @throws InsufficientPrivilegeException
   * @throws UndefinedColumnException
   * @throws UndefinedTableException
   * @throws DuplicateDatabaseException
   * @throws DuplicatePartitionException
   * @throws UndefinedDatabaseException
   * @throws UndefinedPartitionMethodException
   * @throws UndefinedPartitionException
   * @throws NotImplementedException
   *
   * @see AlterTableDesc
   */
  void alterTable(AlterTableDesc desc)
      throws DuplicateColumnException,
      DuplicateTableException,
      InsufficientPrivilegeException,
      UndefinedColumnException,
      UndefinedTableException,
      DuplicateDatabaseException,
      DuplicatePartitionException,
      UndefinedDatabaseException,
      UndefinedPartitionMethodException,
      UndefinedPartitionException,
       NotImplementedException;

  void updateTableStats(UpdateTableStatsProto stats) throws UndefinedTableException, InsufficientPrivilegeException;
}
