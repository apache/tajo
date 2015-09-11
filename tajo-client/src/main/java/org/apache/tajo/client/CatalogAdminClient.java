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

package org.apache.tajo.client;

import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.exception.*;

import java.io.Closeable;
import java.net.URI;
import java.util.List;

public interface CatalogAdminClient extends Closeable {
  /**
   * Create a database.
   *
   * @param databaseName The database name to be created. This name is case sensitive.
   * @return True if created successfully.
   * @throws DuplicateDatabaseException
   */
  void createDatabase(final String databaseName) throws DuplicateDatabaseException;
  /**
   * Does the database exist?
   *
   * @param databaseName The database name to be checked. This name is case sensitive.
   * @return True if so.
   */
  boolean existDatabase(final String databaseName);
  /**
   * Drop the database
   *
   * @param databaseName The database name to be dropped. This name is case sensitive.
   * @return True if the database is dropped successfully.
   * @throws UndefinedDatabaseException
   */
  void dropDatabase(final String databaseName) throws UndefinedDatabaseException, InsufficientPrivilegeException;

  List<String> getAllDatabaseNames();

  /**
   * Does the table exist?
   *
   * @param tableName The table name to be checked. This name is case sensitive.
   * @return True if so.
   */
  boolean existTable(final String tableName);

  /**
   * Create an external table.
   *
   * @param tableName The table name to be created. This name is case sensitive. This name can be qualified or not.
   *                  If the table name is not qualified, the current database in the session will be used.
   * @param schema The schema
   * @param path The external table location
   * @param meta Table meta
   * @return the created table description.
   * @throws DuplicateTableException
   */
  TableDesc createExternalTable(final String tableName, final Schema schema, final URI path,
                                       final TableMeta meta)
      throws DuplicateTableException, UnavailableTableLocationException, InsufficientPrivilegeException;

  /**
   * Create an external table.
   *
   * @param tableName The table name to be created. This name is case sensitive. This name can be qualified or not.
   *                  If the table name is not qualified, the current database in the session will be used.
   * @param schema The schema
   * @param path The external table location
   * @param meta Table meta
   * @param partitionMethodDesc Table partition description
   * @return the created table description.
   * @throws DuplicateTableException
   */
  TableDesc createExternalTable(final String tableName, final Schema schema, final URI path,
                                       final TableMeta meta, final PartitionMethodDesc partitionMethodDesc)
      throws DuplicateTableException, InsufficientPrivilegeException, UnavailableTableLocationException;

  /**
   * Drop a table
   *
   * @param tableName The table name to be dropped. This name is case sensitive.
   * @return True if the table is dropped successfully.
   * @throws InsufficientPrivilegeException
   */
  void dropTable(final String tableName) throws UndefinedTableException, InsufficientPrivilegeException;

  /**
   * Drop a table.
   *
   * @param tableName The table name to be dropped. This name is case sensitive.
   * @param purge If purge is true, this call will remove the entry in catalog as well as the table contents.
   * @return True if the table is dropped successfully.
   * @throws UndefinedTableException
   * @throws InsufficientPrivilegeException
   */
  void dropTable(final String tableName, final boolean purge) throws UndefinedTableException,
      InsufficientPrivilegeException;

  /**
   * Get a list of table names.
   *
   * @param databaseName The database name to show all tables. This name is case sensitive.
   *                     If it is null, this method will show all tables
   *                     in the current database of this session.
   */
  List<String> getTableList(@Nullable final String databaseName);

  /**
   * Get a table description
   *
   * @param tableName The table name to get. This name is case sensitive.
   * @return Table description
   * @throws UndefinedTableException
   */
  TableDesc getTableDesc(final String tableName) throws UndefinedTableException;

  /**
   * Get all partition lists of specified table.
   *
   * @param tableName The table name to get. This name is case sensitive.
   * @return lists of partitions
   */
  List<PartitionDescProto> getAllPartitions(final String tableName) throws UndefinedDatabaseException,
    UndefinedTableException, UndefinedPartitionMethodException;

  List<CatalogProtos.FunctionDescProto> getFunctions(final String functionName);

  IndexDescProto getIndex(final String indexName);

  boolean existIndex(final String indexName);

  List<IndexDescProto> getIndexes(final String tableName);

  boolean hasIndexes(final String tableName);

  IndexDescProto getIndex(final String tableName, final String[] columnNames);

  boolean existIndex(final String tableName, final String[] columnName);

  boolean dropIndex(final String indexName);
}
