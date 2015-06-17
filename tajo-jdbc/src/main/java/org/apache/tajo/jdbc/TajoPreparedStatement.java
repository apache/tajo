package org.apache.tajo.jdbc; /**
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

import org.apache.tajo.client.TajoClient;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;

/**
 * TajoPreparedStatement.
 *
 */
public class TajoPreparedStatement extends TajoStatement implements PreparedStatement {

  private final String sql;

  /**
   * save the SQL parameters {paramLoc:paramValue}
   */
  private final HashMap<Integer, String> parameters=new HashMap<Integer, String>();

  /**
   * keep the current ResultRet update count
   */
  private int updateCount = 0;

  /**
   *
   */
  public TajoPreparedStatement(JdbcConnection conn,
                               TajoClient tajoClient,
                               String sql) {
    super(conn, tajoClient);
    this.sql = sql;
  }

  @Override
  public void addBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("addBatch");
  }

  @Override
  public void clearParameters() throws SQLException {
    checkConnection("Can't clear parameters");
    this.parameters.clear();
  }

  @Override
  public boolean execute() throws SQLException {
    resultSet = executeImmediate(sql);
    return resultSet != null;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return executeImmediate(sql);
  }

  @Override
  public int executeUpdate() throws SQLException {
    executeImmediate(sql);
    return updateCount;
  }

  protected TajoResultSetBase executeImmediate(String sql) throws SQLException {
    checkConnection("Can't execute");

    try {
      if (sql.contains("?")) {
        sql = updateSql(sql, parameters);
      }
      return (TajoResultSetBase) executeSQL(sql);
    } catch (Exception e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  /**
   * update the SQL string with parameters set by setXXX methods of {@link java.sql.PreparedStatement}
   *
   * @param sql
   * @param parameters
   * @return updated SQL string
   */
  private String updateSql(final String sql, HashMap<Integer, String> parameters) {

    StringBuilder newSql = new StringBuilder(sql);

    int paramLoc = 1;
    while (getCharIndexFromSqlByParamLocation(sql, '?', paramLoc) > 0) {
      // check the user has set the needs parameters
      if (parameters.containsKey(paramLoc)) {
        int tt = getCharIndexFromSqlByParamLocation(newSql.toString(), '?', 1);
        newSql.deleteCharAt(tt);
        newSql.insert(tt, parameters.get(paramLoc));
      }
      paramLoc++;
    }

    return newSql.toString();

  }

  /**
   * Get the index of given char from the SQL string by parameter location
   * </br> The -1 will be return, if nothing found
   *
   * @param sql
   * @param cchar
   * @param paramLoc
   * @return
   */
  private int getCharIndexFromSqlByParamLocation(final String sql, final char cchar, final int paramLoc) {
    int signalCount = 0;
    int charIndex = -1;
    int num = 0;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '\'' || c == '\\')// record the count of char "'" and char "\"
      {
        signalCount++;
      } else if (c == cchar && signalCount % 2 == 0) {// check if the ? is really the parameter
        num++;
        if (num == paramLoc) {
          charIndex = i;
          break;
        }
      }
    }
    return charIndex;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkConnection("Can't get metadata");
    if(resultSet != null) {
      return resultSet.getMetaData();
    } else {
      return null;
    }
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException("getParameterMetaData not supported");
  }

  @Override
  public void setArray(int i, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setArray not supported");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setBigDecimal not supported");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
  }

  @Override
  public void setBlob(int i, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setBlob not supported");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("setBlob not supported");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
          throws SQLException {
    throw new SQLFeatureNotSupportedException("setBlob not supported");
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    checkConnection("Can't set parameters");
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setByte not supported");
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setBytes not supported");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
  }

  @Override
  public void setClob(int i, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setClob not supported");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("setClob not supported");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setClob not supported");
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setDate not supported");
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("setDate not supported");
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    checkConnection("Can't set parameters");
    this.parameters.put(parameterIndex,"" + x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    checkConnection("Can't set parameters");
    this.parameters.put(parameterIndex,"" + x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    checkConnection("Can't set parameters");
    this.parameters.put(parameterIndex,"" + x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    checkConnection("Can't set parameters");
    this.parameters.put(parameterIndex,"" + x);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLFeatureNotSupportedException("setNClob not supported");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("setNClob not supported");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("setNClob not supported");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLFeatureNotSupportedException("setNString not supported");
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("setNull not supported");
  }

  @Override
  public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {
    throw new SQLFeatureNotSupportedException("setNull not supported");
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setObject not supported");
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setObject not supported");
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setObject not supported");
  }

  @Override
  public void setRef(int i, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setRef not supported");
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setRowId not supported");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("setSQLXML not supported");
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    checkConnection("Can't set parameters");
    this.parameters.put(parameterIndex,"" + x);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    checkConnection("Can't set parameters");
     x=x.replace("'", "\\'");
     this.parameters.put(parameterIndex,"'" + x +"'");
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setTime not supported");
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("setTime not supported");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setTimestamp not supported");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setTimestamp not supported");
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLFeatureNotSupportedException("setURL not supported");
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("setUnicodeStream not supported");
  }
}
