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
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.DatabaseProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescriptorProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableOptionProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TablePartitionProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatsProto;

import java.io.Closeable;

import org.apache.tajo.catalog.exception.CatalogException;

import java.util.Collection;
import java.util.List;

import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto;
import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionMethodProto;
import static org.apache.tajo.catalog.proto.CatalogProtos.TablespaceProto;

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
  
  List<TableOptionProto> getAllTableOptions() throws CatalogException;
  
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

  /**
   * PartitionedTableRewriter take a look into partition directories for rewriting filter conditions. But if there
   * are lots of sub directories on HDFS, such as, more than 10,000 directories,
   * it might be cause overload to NameNode. Thus, CatalogStore need to provide partition directories for specified
   * filter conditions. This scan right partition directories on CatalogStore with where clause.
   *
   * For examples, users set parameters as following:
   *  - databaseName: default
   *  - tableName: table1
   *  - directSql: COLUMN_NAME = 'col1' AND PARTITION_VALUE = '3' OR COLUMN_NAME = 'col2' AND PARTITION_VALUE IS NOT
   *  NULL OR COLUMN_NAME = 'col3' AND PARTITION_VALUE IN ('1', '2', '3')
   *
   * @param databaseName the database name
   * @param tableName the table name
   * @param directSql where clause (included target column name and partition value, this should be DNF)
   * @return list of TablePartitionProto
   * @throws CatalogException
   */
  List<TablePartitionProto> getPartitionsByDirectSql(String databaseName, String tableName,
                                               String directSql) throws CatalogException;

  List<TablePartitionProto> getAllPartitions() throws CatalogException;

  /**************************** INDEX *******************************/
  void createIndex(IndexDescProto proto) throws CatalogException;
  
  void dropIndex(String databaseName, String indexName) throws CatalogException;
  
  IndexDescProto getIndexByName(String databaseName, String indexName) throws CatalogException;
  
  IndexDescProto getIndexByColumn(String databaseName, String tableName, String columnName)
      throws CatalogException;
  
  boolean existIndexByName(String databaseName, String indexName) throws CatalogException;
  
  boolean existIndexByColumn(String databaseName, String tableName, String columnName)
      throws CatalogException;

  IndexDescProto [] getIndexes(String databaseName, String tableName) throws CatalogException;
  
  List<IndexProto> getAllIndexes() throws CatalogException;

  /************************** FUNCTION *****************************/

  
  void addFunction(FunctionDesc func) throws CatalogException;
  
  void deleteFunction(FunctionDesc func) throws CatalogException;
  
  void existFunction(FunctionDesc func) throws CatalogException;
  
  List<String> getAllFunctionNames() throws CatalogException;
}
