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

/**
 * 
 */
package org.apache.tajo.jdbc;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.client.ResultSetUtil;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.type.TajoTypeUtil;
import org.apache.tajo.schema.IdentifierUtil;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class TajoResultSetMetaData implements ResultSetMetaData {
  Schema schema;


  public TajoResultSetMetaData(Schema schema) {
    this.schema = schema;
  }

  @Override
  public boolean isWrapperFor(Class<?> clazz) throws SQLException {
    throw new SQLFeatureNotSupportedException("isWrapperFor not supported");
  }

  @Override
  public <T> T unwrap(Class<T> clazz) throws SQLException {
    throw new SQLFeatureNotSupportedException("unwrap not supported");
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    Column c = schema.getColumn(column - 1);
    if (IdentifierUtil.isFQColumnName(c.getQualifiedName())) {
      return IdentifierUtil.splitFQTableName(c.getQualifier())[0];
    }
    return "";
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return schema.getColumn(column - 1).getClass().getName();
  }

  @Override
  public int getColumnCount() throws SQLException {
    if(schema == null) {
      return 0;
    }
    return schema.size();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return ResultSetUtil.columnDisplaySize(getColumnType(column));
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return schema.getColumn(column - 1).getQualifiedName();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return schema.getColumn(column - 1).getSimpleName();
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    DataType type = schema.getColumn(column - 1).getDataType();

    return ResultSetUtil.tajoTypeToSqlType(type);
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    DataType type = schema.getColumn(column - 1).getDataType();

    return ResultSetUtil.toSqlType(type);
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return ResultSetUtil.columnDisplaySize(getColumnType(column));
  }

  @Override
  public int getScale(int column) throws SQLException {
    return ResultSetUtil.columnScale(getColumnType(column));
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return "";
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return schema.getColumn(column - 1).getQualifier();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    DataType type = schema.getColumn(column - 1).getDataType();
    return TajoTypeUtil.isSigned(type.getType());
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }
}
