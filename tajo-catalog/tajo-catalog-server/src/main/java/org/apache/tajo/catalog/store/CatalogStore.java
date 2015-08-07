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

package org.apache.tajo.catalog.store;

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.exception.TajoException;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

public interface CatalogStore extends Closeable {
  /*************************** Tablespace ******************************/
  void createTablespace(String spaceName, String spaceUri) throws TajoException;

  boolean existTablespace(String spaceName) throws TajoException;

  void dropTablespace(String spaceName) throws TajoException;

  Collection<String> getAllTablespaceNames() throws TajoException;
  
  List<TablespaceProto> getTablespaces() throws TajoException;

  TablespaceProto getTablespace(String spaceName) throws TajoException;

  void alterTablespace(AlterTablespaceProto alterProto) throws TajoException;

  /*************************** Database ******************************/
  void createDatabase(String databaseName, String tablespaceName) throws TajoException;

  boolean existDatabase(String databaseName) throws TajoException;

  void dropDatabase(String databaseName) throws TajoException;

  Collection<String> getAllDatabaseNames() throws TajoException;
  
  List<DatabaseProto> getAllDatabases() throws TajoException;

  /*************************** TABLE ******************************/
  void createTable(CatalogProtos.TableDescProto desc) throws TajoException;
  
  boolean existTable(String databaseName, String tableName) throws TajoException;
  
  void dropTable(String databaseName, String tableName) throws TajoException;
  
  CatalogProtos.TableDescProto getTable(String databaseName, String tableName) throws TajoException;
  
  List<String> getAllTableNames(String databaseName) throws TajoException;

  void alterTable(CatalogProtos.AlterTableDescProto alterTableDescProto) throws TajoException;
  
  List<TableDescriptorProto> getAllTables() throws TajoException;

  List<TableOptionProto> getAllTableProperties() throws TajoException;
  
  List<TableStatsProto> getAllTableStats() throws TajoException;
  
  List<ColumnProto> getAllColumns() throws TajoException;

  void updateTableStats(CatalogProtos.UpdateTableStatsProto statsProto) throws TajoException;

  /************************ PARTITION METHOD **************************/
  void addPartitionMethod(PartitionMethodProto partitionMethodProto) throws TajoException;

  PartitionMethodProto getPartitionMethod(String databaseName, String tableName)
      throws TajoException;

  boolean existPartitionMethod(String databaseName, String tableName) throws TajoException;

  void dropPartitionMethod(String dbName, String tableName) throws TajoException;


  /************************** PARTITIONS *****************************/
  /**
   * Get all partitions of a table
   * @param tableName the table name
   * @return
   * @throws TajoException
   */
  List<CatalogProtos.PartitionDescProto> getPartitions(String databaseName, String tableName) throws TajoException;

  CatalogProtos.PartitionDescProto getPartition(String databaseName, String tableName,
                                                String partitionName) throws TajoException;

  List<TablePartitionProto> getAllPartitions() throws TajoException;

  void addPartitions(String databaseName, String tableName, List<CatalogProtos.PartitionDescProto> partitions
    , boolean ifNotExists) throws TajoException;

  /**************************** INDEX *******************************/
  void createIndex(IndexDescProto proto) throws TajoException;
  
  void dropIndex(String databaseName, String indexName) throws TajoException;
  
  IndexDescProto getIndexByName(String databaseName, String indexName) throws TajoException;

  IndexDescProto getIndexByColumns(String databaseName, String tableName, String[] columnNames)
      throws TajoException;
  
  boolean existIndexByName(String databaseName, String indexName) throws TajoException;

  boolean existIndexByColumns(String databaseName, String tableName, String[] columnNames)
      throws TajoException;

  List<String> getAllIndexNamesByTable(String databaseName, String tableName) throws TajoException;

  boolean existIndexesByTable(String databaseName, String tableName) throws TajoException;

  List<IndexDescProto> getAllIndexes() throws TajoException;

  /************************** FUNCTION *****************************/

  
  void addFunction(FunctionDesc func) throws TajoException;
  
  void deleteFunction(FunctionDesc func) throws TajoException;
  
  void existFunction(FunctionDesc func) throws TajoException;
  
  List<String> getAllFunctionNames() throws TajoException;
}
