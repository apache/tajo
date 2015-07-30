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

import java.io.Closeable;

import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueProto;

import java.util.Collection;
import java.util.List;

public interface CatalogStore extends Closeable {
  /*************************** Tablespace ******************************/
  void createTablespace(String spaceName, String spaceUri) throws CatalogException;

  boolean existTablespace(String spaceName) throws CatalogException;

  void dropTablespace(String spaceName) throws CatalogException;

  Collection<String> getAllTablespaceNames() throws CatalogException;
  
  List<TablespaceProto> getTablespaces() throws CatalogException;

  TablespaceProto getTablespace(String spaceName) throws CatalogException;

  void alterTablespace(AlterTablespaceProto alterProto) throws CatalogException;

  /*************************** Database ******************************/
  void createDatabase(String databaseName, String tablespaceName) throws CatalogException;

  boolean existDatabase(String databaseName) throws CatalogException;

  void dropDatabase(String databaseName) throws CatalogException;

  Collection<String> getAllDatabaseNames() throws CatalogException;
  
  List<DatabaseProto> getAllDatabases() throws CatalogException;

  /*************************** TABLE ******************************/
  void createTable(CatalogProtos.TableDescProto desc) throws CatalogException;
  
  boolean existTable(String databaseName, String tableName) throws CatalogException;
  
  void dropTable(String databaseName, String tableName) throws CatalogException;
  
  CatalogProtos.TableDescProto getTable(String databaseName, String tableName) throws CatalogException;
  
  List<String> getAllTableNames(String databaseName) throws CatalogException;

  void alterTable(CatalogProtos.AlterTableDescProto alterTableDescProto) throws CatalogException;
  
  List<TableDescriptorProto> getAllTables() throws CatalogException;

  List<TableOptionProto> getAllTableProperties() throws CatalogException;
  
  List<TableStatsProto> getAllTableStats() throws CatalogException;
  
  List<ColumnProto> getAllColumns() throws CatalogException;

  void updateTableStats(CatalogProtos.UpdateTableStatsProto statsProto) throws CatalogException;

  /************************ PARTITION METHOD **************************/
  void addPartitionMethod(PartitionMethodProto partitionMethodProto) throws CatalogException;

  PartitionMethodProto getPartitionMethod(String databaseName, String tableName)
      throws CatalogException;

  boolean existPartitionMethod(String databaseName, String tableName) throws CatalogException;

  void dropPartitionMethod(String dbName, String tableName) throws CatalogException;


  /************************** PARTITIONS *****************************/
  /**
   * Get all partitions of a table
   * @param tableName the table name
   * @return
   * @throws CatalogException
   */
  List<CatalogProtos.PartitionDescProto> getPartitions(String databaseName, String tableName) throws CatalogException;

  CatalogProtos.PartitionDescProto getPartition(String databaseName, String tableName,
                                                String partitionName) throws CatalogException;

  List<TablePartitionProto> getAllPartitions() throws CatalogException;

  /**************************** INDEX *******************************/
  void createIndex(IndexDescProto proto) throws CatalogException;
  
  void dropIndex(String databaseName, String indexName) throws CatalogException;
  
  IndexDescProto getIndexByName(String databaseName, String indexName) throws CatalogException;

  IndexDescProto getIndexByColumns(String databaseName, String tableName, String[] columnNames)
      throws CatalogException;
  
  boolean existIndexByName(String databaseName, String indexName) throws CatalogException;

  boolean existIndexByColumns(String databaseName, String tableName, String[] columnNames)
      throws CatalogException;

  List<String> getAllIndexNamesByTable(String databaseName, String tableName) throws CatalogException;

  boolean existIndexesByTable(String databaseName, String tableName) throws CatalogException;

  List<IndexDescProto> getAllIndexes() throws CatalogException;

  /************************** FUNCTION *****************************/

  
  void addFunction(FunctionDesc func) throws CatalogException;
  
  void deleteFunction(FunctionDesc func) throws CatalogException;
  
  void existFunction(FunctionDesc func) throws CatalogException;
  
  List<String> getAllFunctionNames() throws CatalogException;
}
