/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog.store;

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.exception.*;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

public interface CatalogStore extends Closeable {
  String getUri();

  /*************************** Tablespace ******************************/
  void createTablespace(String spaceName, String spaceUri) throws DuplicateTablespaceException;

  boolean existTablespace(String spaceName);

  void dropTablespace(String spaceName) throws UndefinedTablespaceException, UndefinedTableException;

  Collection<String> getAllTablespaceNames();

  List<TablespaceProto> getTablespaces();

  TablespaceProto getTablespace(String spaceName) throws UndefinedTablespaceException;

  void alterTablespace(AlterTablespaceProto alterProto) throws UndefinedTablespaceException;

  /*************************** Database ******************************/
  void createDatabase(String databaseName, String tablespaceName) throws UndefinedTablespaceException,
      DuplicateDatabaseException;

  boolean existDatabase(String databaseName);

  void dropDatabase(String databaseName) throws UndefinedDatabaseException, UndefinedTableException;

  Collection<String> getAllDatabaseNames();

  List<DatabaseProto> getAllDatabases();

  /*************************** TABLE ******************************/
  void createTable(CatalogProtos.TableDescProto desc) throws UndefinedDatabaseException, DuplicateTableException;

  boolean existTable(String databaseName, String tableName) throws UndefinedDatabaseException;

  void dropTable(String databaseName, String tableName) throws UndefinedDatabaseException, UndefinedTableException;

  CatalogProtos.TableDescProto getTable(String databaseName, String tableName) throws UndefinedDatabaseException,
      UndefinedTableException;

  List<String> getAllTableNames(String databaseName) throws UndefinedDatabaseException;

  void alterTable(CatalogProtos.AlterTableDescProto alterTableDescProto) throws UndefinedDatabaseException,
      DuplicateTableException, DuplicateColumnException, DuplicatePartitionException, UndefinedPartitionException,
      UndefinedTableException, UndefinedColumnException, UndefinedPartitionMethodException;

  List<TableDescriptorProto> getAllTables();

  List<TableOptionProto> getAllTableProperties();

  List<TableStatsProto> getAllTableStats();

  List<ColumnProto> getAllColumns();

  void updateTableStats(CatalogProtos.UpdateTableStatsProto statsProto) throws UndefinedDatabaseException, UndefinedTableException;

  /************************ PARTITION METHOD **************************/
  PartitionMethodProto getPartitionMethod(String databaseName, String tableName) throws UndefinedDatabaseException,
      UndefinedTableException, UndefinedPartitionMethodException;

  boolean existPartitionMethod(String databaseName, String tableName) throws UndefinedDatabaseException,
      UndefinedTableException;

  /************************** PARTITIONS *****************************/
  /**
   * Get all partitions of a table
   * @param tableName the table name
   * @return
   * @throws TajoException
   */
  List<CatalogProtos.PartitionDescProto> getPartitions(String databaseName, String tableName) throws
      UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException;

  CatalogProtos.PartitionDescProto getPartition(String databaseName, String tableName,
                                                String partitionName)
      throws UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionException,
      UndefinedPartitionMethodException;

  List<TablePartitionProto> getAllPartitions();

  void addPartitions(String databaseName, String tableName, List<CatalogProtos.PartitionDescProto> partitions
      , boolean ifNotExists) throws UndefinedDatabaseException,
      UndefinedTableException, DuplicatePartitionException, UndefinedPartitionException,
      UndefinedPartitionMethodException;

  /**************************** INDEX *******************************/
  void createIndex(IndexDescProto proto) throws UndefinedDatabaseException, UndefinedTableException,
      DuplicateIndexException;

  void dropIndex(String databaseName, String indexName) throws UndefinedDatabaseException,
      UndefinedTableException, UndefinedIndexException;

  IndexDescProto getIndexByName(String databaseName, String indexName) throws UndefinedDatabaseException,
      UndefinedIndexException;

  IndexDescProto getIndexByColumns(String databaseName, String tableName, String[] columnNames) throws
      UndefinedDatabaseException, UndefinedTableException, UndefinedIndexException
      ;

  boolean existIndexByName(String databaseName, String indexName) throws UndefinedDatabaseException;

  boolean existIndexByColumns(String databaseName, String tableName, String[] columnNames) throws
      UndefinedDatabaseException, UndefinedTableException;

  List<String> getAllIndexNamesByTable(String databaseName, String tableName) throws UndefinedDatabaseException, UndefinedTableException;

  boolean existIndexesByTable(String databaseName, String tableName)
      throws UndefinedDatabaseException, UndefinedTableException;

  List<IndexDescProto> getAllIndexes() throws UndefinedDatabaseException;

  /************************** FUNCTION *****************************/


  void addFunction(FunctionDesc func);

  void deleteFunction(FunctionDesc func);

  void existFunction(FunctionDesc func);

  List<String> getAllFunctionNames();
}
