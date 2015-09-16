/*
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

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.exception.UndefinedTablespaceException;
import org.apache.tajo.util.Pair;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Collections2.filter;

/**
 * Linked Meta Data Manager which manages all meta data providers and access methods for them.
 */
public class LinkedMetadataManager {
  private ImmutableMap<String, MetadataProvider> providerMap;

  /**
   * Initialize Linked Metadata
   *
   * @param providers A collection of Metadata providers.
   */
  public LinkedMetadataManager(Collection<MetadataProvider> providers) {
    Map<String, MetadataProvider> builder = Maps.newHashMap();

    for (MetadataProvider p : providers) {
      builder.put(p.getDatabaseName(), p);
    }

    this.providerMap = ImmutableMap.copyOf(builder);
  }

  /**
   * Get all tablespace names
   *
   * @return A collection of tablespace names
   */
  public Collection<String> getTablespaceNames() {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (MetadataProvider p : providerMap.values()) {
      builder.add(p.getTablespaceName());
    }
    return builder.build();
  }

  /**
   * Get a tablespace by name.
   *
   * @param spaceName Tablespace name
   * @return A pair where the first value is tablespace name and the second value is an URI.
   */
  public Optional<Pair<String, URI>> getTablespace(final String spaceName) {
    Collection<MetadataProvider> filtered = filter(providerMap.values(),
        new Predicate<MetadataProvider>() {
          @Override
          public boolean apply(@Nullable MetadataProvider input) {
            return input.getTablespaceName().equals(spaceName);
          }
        });

    if (filtered.isEmpty()) {
      return Optional.absent();
    } else {
      MetadataProvider found = filtered.iterator().next();
      return Optional.of(new Pair<String, URI>(found.getTablespaceName(), found.getTablespaceUri()));
    }
  }

  /**
   * Return all tablespaces.
   *
   * @return A list of pairs, each pair represents a tablespace, where the first value is tablespace name.
   *         the second value is an URI.
   */
  public Collection<Pair<String, URI>> getTablespaces() {
    ImmutableList.Builder<Pair<String, URI>> builder = ImmutableList.builder();
    for (MetadataProvider p : providerMap.values()) {
      builder.add(new Pair<String, URI>(p.getDatabaseName(), p.getTablespaceUri()));
    }
    return builder.build();
  }

  /**
   * Check if the tablespace exists.
   *
   * @param tablespaceName
   * @return True if the tablespace exists.
   */
  public boolean existsTablespace(String tablespaceName) {
    for (MetadataProvider provider : providerMap.values()) {
      if (provider.getTablespaceName().equals(tablespaceName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return all database names
   *
   * @return A collection of database names
   */
  public Collection<String> getDatabases() {
    return providerMap.keySet();
  }

  /**
   * check if the database exists.
   *
   * @param dbName Database name
   * @return True if the database exists. Otherwise, False.
   */
  public boolean existsDatabase(String dbName) {
    return providerMap.containsKey(dbName);
  }

  /**
   * check if the database exists. Otherwise, it throws an exception
   *
   * @param dbName Database name
   * @throws UndefinedDatabaseException
   */
  private void ensureIfDBExists(String dbName) throws UndefinedDatabaseException {
    if (!providerMap.containsKey(dbName)) {
      throw new UndefinedDatabaseException(dbName);
    }
  }

  /**
   * Get schema names
   *
   * @param dbName Database name
   * @return A list of schema names
   * @throws UndefinedDatabaseException
   */
  public Collection<String> getSchemas(@Nullable String dbName) throws UndefinedDatabaseException {
    ensureIfDBExists(dbName);

    return providerMap.get(dbName).getSchemas();
  }

  /**
   * Get all table names matched to a given pattern
   *
   * @param dbName Database name
   * @param schemaPattern a schema name pattern; must match the schema name
   *        as it is stored in the database; "" retrieves tables without a schema;
   *        <code>null</code> means that the schema name should not be used to narrow the search
   * @param tablePattern a table name pattern: must match the table name as it is stored in the meta data provider;
   *                     "" retrieves tables without a schema; <code>null</code> allows all schema names.
   *
   * @return
   */
  public Collection<String> getTableNames(String dbName,
                                          @Nullable final String schemaPattern,
                                          @Nullable final String tablePattern) throws UndefinedDatabaseException {
    ensureIfDBExists(dbName);
    return providerMap.get(dbName).getTables(schemaPattern, tablePattern);
  }

  /**
   * Create a table
   *
   * @param desc table description
   * @throws InsufficientPrivilegeException
   */
  public void createTable(TableDesc desc) throws InsufficientPrivilegeException {
    // TODO - currently, meta data provider is read only.
    throw new InsufficientPrivilegeException("create a table in external metadata store");
  }

  /**
   * Drop table
   *
   * @param databaseName Database name
   * @param schemaName a schema name; "" drops the table without a schema.
   * @param tableName Table name
   * @throws InsufficientPrivilegeException
   */
  public void dropTable(String databaseName, String schemaName, String tableName)
      throws InsufficientPrivilegeException {
    // TODO - currently, meta data provider is read only.
    throw new InsufficientPrivilegeException("drop any table in external metadata store");
  }

  /**
   * Check if the table exists
   *
   * @param dbName Database name
   * @param schemaName a schema name; "" checks the table without a schema.
   * @param tableName a table name
   * @return True if the table exists. Otherwise, False
   */
  public boolean existsTable(String dbName, String schemaName, String tableName) {
    if (providerMap.containsKey(dbName)) {
      return providerMap.get(dbName).getTables(schemaName, tableName).contains(tableName);
    }

    return false;
  }

  /**
   * Get Table description
   *
   * @param dbName Database name
   * @param schemaName a schema name; "" checks the table without a schema.
   * @param tbName Table name
   * @return Table description
   */
  public TableDesc getTable(String dbName, String schemaName, String tbName)
      throws UndefinedDatabaseException, UndefinedTablespaceException {

    ensureIfDBExists(dbName);

    return providerMap.get(dbName).getTableDesc(schemaName, tbName);
  }
}
