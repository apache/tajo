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

import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;

import java.util.Collection;
import java.util.List;

public interface Schema extends ProtoObject<SchemaProto>, Cloneable, GsonObject {

  /**
   * Set a qualifier to this schema.
   * This changes the qualifier of all columns except for not-qualified columns.
   *
   * @param qualifier The qualifier
   */
  void setQualifier(String qualifier);
	
	int size();

  Column getColumn(int id);

  Column getColumn(Column column);

  int getIndex(Column column);

  /**
   * Get a column by a given name.
   *
   * @param name The column name to be found.
   * @return The column matched to a given column name.
   */
  Column getColumn(String name);

	int getColumnId(String name);

  int getColumnIdByName(String colName);

  /**
   * Get root columns, meaning all columns except for nested fields.
   *
   * @return A list of root columns
   */
	List<Column> getRootColumns();

  /**
   * Get all columns, including all nested fields
   *
   * @return A list of all columns
   */
  List<Column> getAllColumns();

  boolean contains(String name);

  boolean contains(Column column);
	
	boolean containsByQualifiedName(String qualifiedName);

  boolean containsByName(String colName);

  boolean containsAll(Collection<Column> columns);

  /**
   * Return TRUE if any column in <code>columns</code> is included in this schema.
   *
   * @param columns Columns to be checked
   * @return true if any column in <code>columns</code> is included in this schema.
   *         Otherwise, false.
   */
  boolean containsAny(Collection<Column> columns);


  @Override
	boolean equals(Object o);

  Object clone() throws CloneNotSupportedException;

	@Override
	SchemaProto getProto();

  @Override
	String toString();

  @Override
	String toJson();

  Column [] toArray();
}