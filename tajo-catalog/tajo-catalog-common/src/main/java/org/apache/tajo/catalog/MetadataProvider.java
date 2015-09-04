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

import org.apache.tajo.exception.UndefinedTablespaceException;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collection;

public interface MetadataProvider {
  /**
   * Get a Tablespace name
   *
   * @return Tablespace name
   */
  String getTablespaceName();

  /**
   * Get a tablespace URI
   *
   * @return Table space URI
   */
  URI getTablespaceUri();

  /**
   * Return a database name
   *
   * @return Database name
   */
  String getDatabaseName();

  /**
   * Get a list of schema names
   * @return All schema names in this database
   */
  Collection<String> getSchemas();

  /**
   * get Table names matched to a given patterns
   *
   * @param schemaPattern Schema name pattern
   * @param tablePattern Table name pattern
   * @return All matched table names
   */
  Collection<String> getTables(@Nullable String schemaPattern, @Nullable String tablePattern);

  /**
   * Get a table descriptor
   *
   * @param schemaName schema name
   * @param tableName table name
   * @return TableDesc
   * @throws UndefinedTablespaceException
   */
  TableDesc getTableDesc(String schemaName, String tableName) throws UndefinedTablespaceException;
}
