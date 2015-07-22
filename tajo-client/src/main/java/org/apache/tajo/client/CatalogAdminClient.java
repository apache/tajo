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

import java.io.Closeable;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;

public interface CatalogAdminClient extends Closeable {
  /**
   * Create a database.
   *
   * @param databaseName The database name to be created. This name is case sensitive.
   * @return True if created successfully.
   * @throws java.sql.SQLException
   */
  boolean createDatabase(final String databaseName) throws SQLException;
  /**
   * Does the database exist?
   *
   * @param databaseName The database name to be checked. This name is case sensitive.
   * @return True if so.
   * @throws java.sql.SQLException
   */
  boolean existDatabase(final String databaseName) throws SQLException;
  /**
   * Drop the database
   *
   * @param databaseName The database name to be dropped. This name is case sensitive.
   * @return True if the database is dropped successfully.
   * @throws java.sql.SQLException
   */
  boolean dropDatabase(final String databaseName) throws SQLException;

  List<String> getAllDatabaseNames() throws SQLException;

  /**
   * Does the table exist?
   *
   * @param tableName The table name to be checked. This name is case sensitive.
   * @return True if so.
   */
  boolean existTable(final String tableName) throws SQLException;

  /**
   * Create an external table.
   *
   * @param tableName The table name to be created. This name is case sensitive. This name can be qualified or not.
   *                  If the table name is not qualified, the current database in the session will be used.
   * @param schema The schema
   * @param path The external table location
   * @param meta Table meta
   * @return the created table description.
   * @throws java.sql.SQLException
   */
  TableDesc createExternalTable(final String tableName, final Schema schema, final URI path,
                                       final TableMeta meta) throws SQLException;

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
   * @throws java.sql.SQLException
   */
  TableDesc createExternalTable(final String tableName, final Schema schema, final URI path,
                                       final TableMeta meta, final PartitionMethodDesc partitionMethodDesc)
      throws SQLException;

  /**
   * Drop a table
   *
   * @param tableName The table name to be dropped. This name is case sensitive.
   * @return True if the table is dropped successfully.
   * @throws java.sql.SQLException
   */
  boolean dropTable(final String tableName) throws SQLException;

  /**
   * Drop a table.
   *
   * @param tableName The table name to be dropped. This name is case sensitive.
   * @param purge If purge is true, this call will remove the entry in catalog as well as the table contents.
   * @return True if the table is dropped successfully.
   * @throws java.sql.SQLException
   */
  boolean dropTable(final String tableName, final boolean purge) throws SQLException;

  /**
   * Get a list of table names.
   *
   * @param databaseName The database name to show all tables. This name is case sensitive.
   *                     If it is null, this method will show all tables
   *                     in the current database of this session.
   * @throws java.sql.SQLException
   */
  List<String> getTableList(@Nullable final String databaseName) throws SQLException;

  /**
   * Get a table description
   *
   * @param tableName The table name to get. This name is case sensitive.
   * @return Table description
   * @throws java.sql.SQLException
   */
  TableDesc getTableDesc(final String tableName) throws SQLException;

  List<CatalogProtos.FunctionDescProto> getFunctions(final String functionName) throws SQLException;

  IndexDescProto getIndex(final String indexName) throws SQLException;

  boolean existIndex(final String indexName) throws SQLException;

  List<IndexDescProto> getIndexes(final String tableName) throws SQLException;

  boolean hasIndexes(final String tableName) throws SQLException;

  IndexDescProto getIndex(final String tableName, final String[] columnNames) throws SQLException;

  boolean existIndex(final String tableName, final String[] columnName) throws SQLException;

  boolean dropIndex(final String indexName) throws SQLException;
}
