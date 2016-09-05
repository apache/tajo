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
      UndefinedTableException, UndefinedColumnException, UndefinedPartitionMethodException, AmbiguousTableException,
      UnremovableTablePropertyException;

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
   * Check if list of partitions exist on catalog.
   *
   * @param databaseName
   * @param tableName
   * @return
   * @throws UndefinedDatabaseException
   * @throws UndefinedTableException
   * @throws UndefinedPartitionMethodException
   */
  boolean existPartitions(String databaseName, String tableName) throws
    UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException;

  /**
   * Get all partitions of a table
   * @param tableName the table name
   * @return
   * @throws TajoException
   */
  List<CatalogProtos.PartitionDescProto> getPartitionsOfTable(String databaseName, String tableName) throws
    UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException,
    UnsupportedException;

  CatalogProtos.PartitionDescProto getPartition(String databaseName, String tableName,
                                                String partitionName) throws UndefinedDatabaseException,
    UndefinedTableException, UndefinedPartitionMethodException, UndefinedPartitionException;

  /**
   * Get list of partitions matching specified algrbra expression.
   *
   * For example, consider you have a partitioned table for three columns (i.e., col1, col2, col3).
   * Assume that an user want to give a condition WHERE (col1 ='1' or col1 = '100') and col3 > 20 .
   *
   * Then, the algebra expression would be written as following:
   *
   *  {
   *  "LeftExpr": {
   *    "LeftExpr": {
   *      "Qualifier": "default.table1",
   *      "ColumnName": "col3",
   *      "OpType": "Column"
   *    },
   *    "RightExpr": {
   *      "Value": "20.0",
   *      "ValueType": "Unsigned_Integer",
   *      "OpType": "Literal"
   *    },
   *    "OpType": "GreaterThan"
   *  },
   *  "RightExpr": {
   *    "LeftExpr": {
   *      "LeftExpr": {
   *        "Qualifier": "default.table1",
   *        "ColumnName": "col1",
   *        "OpType": "Column"
   *      },
   *      "RightExpr": {
   *        "Value": "1",
   *        "ValueType": "String",
   *        "OpType": "Literal"
   *      },
   *      "OpType": "Equals"
   *    },
   *    "RightExpr": {
   *      "LeftExpr": {
   *        "Qualifier": "default.table1",
   *        "ColumnName": "col1",
   *        "OpType": "Column"
   *      },
   *      "RightExpr": {
   *        "Value": "100",
   *        "ValueType": "String",
   *        "OpType": "Literal"
   *      },
   *      "OpType": "Equals"
   *    },
   *    "OpType": "Or"
   *  },
   *  "OpType": "And"
   * }
   *
   * @param request the database name, the table name, the algebra expression
   * @return list of PartitionDescProto
   * @throws UndefinedDatabaseException
   * @throws UndefinedTableException
   * @throws UndefinedPartitionMethodException
   * @throws UndefinedOperatorException
   */
  List<PartitionDescProto> getPartitionsByAlgebra(PartitionsByAlgebraProto request) throws
    UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException,
    UndefinedOperatorException, UnsupportedException;

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
