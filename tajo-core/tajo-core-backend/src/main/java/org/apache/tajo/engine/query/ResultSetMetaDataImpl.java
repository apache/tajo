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
package org.apache.tajo.engine.query;

import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.UnsupportedException;

import java.nio.channels.UnsupportedAddressTypeException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class ResultSetMetaDataImpl implements ResultSetMetaData {
  private TableMeta meta;
  
  public ResultSetMetaDataImpl(TableMeta meta) {
    this.meta = meta;
  }

  /* (non-Javadoc)
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */
  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */
  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getCatalogName(int)
   */
  @Override
  public String getCatalogName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnClassName(int)
   */
  @Override
  public String getColumnClassName(int column) throws SQLException {
    return meta.getSchema().getColumn(column - 1).getClass().getName();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnCount()
   */
  @Override
  public int getColumnCount() throws SQLException {
    return meta.getSchema().getColumnNum();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
   */
  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnLabel(int)
   */
  @Override
  public String getColumnLabel(int column) throws SQLException {
    return meta.getSchema().getColumn(column - 1).getQualifiedName();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnName(int)
   */
  @Override
  public String getColumnName(int column) throws SQLException {
    return meta.getSchema().getColumn(column - 1).getColumnName();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnType(int)
   */
  @Override
  public int getColumnType(int column) throws SQLException {
    // TODO
    DataType type = meta.getSchema().getColumn(column - 1).getDataType();
    switch (type.getType()) {
      case BOOLEAN:
        return Types.BOOLEAN;
      case INT1:
        return Types.TINYINT;
      case INT2:
        return Types.SMALLINT;
      case INT4:
        return Types.INTEGER;
      case INT8:
        return Types.BIGINT;
      case FLOAT4:
        return Types.FLOAT;
      case FLOAT8:
        return Types.DOUBLE;
      case DECIMAL:
        return Types.DECIMAL;
      case VARBINARY:
        return Types.VARBINARY;
      case CHAR:
        return Types.CHAR;
      case DATE:
        return Types.DATE;
      case VARCHAR:
        return Types.VARCHAR;
      case TEXT:
        return Types.VARCHAR;
      default:
        throw new UnsupportedException();
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
   */
  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return meta.getSchema().getColumn(column - 1).
        getDataType().getClass().getCanonicalName();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getPrecision(int)
   */
  @Override
  public int getPrecision(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getScale(int)
   */
  @Override
  public int getScale(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getSchemaName(int)
   */
  @Override
  public String getSchemaName(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getTableName(int)
   */
  @Override
  public String getTableName(int column) throws SQLException {
    return meta.getSchema().getColumn(column - 1).getQualifier();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
   */
  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
   */
  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isCurrency(int)
   */
  @Override
  public boolean isCurrency(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
   */
  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isNullable(int)
   */
  @Override
  public int isNullable(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isReadOnly(int)
   */
  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isSearchable(int)
   */
  @Override
  public boolean isSearchable(int column) throws SQLException {
    // TODO
    return true;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isSigned(int)
   */
  @Override
  public boolean isSigned(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isWritable(int)
   */
  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }
}
