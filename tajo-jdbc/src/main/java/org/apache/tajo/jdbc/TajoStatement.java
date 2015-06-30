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
package org.apache.tajo.jdbc;

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;
import org.apache.tajo.QueryId;
import org.apache.tajo.SessionVars;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.ipc.ClientProtos;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tajo.client.ClientErrorUtil.isError;

public class TajoStatement implements Statement {
  protected JdbcConnection conn;
  protected TajoClient tajoClient;
  protected int fetchSize = SessionVars.FETCH_ROWNUM.getConfVars().defaultIntVal;

  /**
   * We need to keep a reference to the result set to support the following:
   * <code>
   * statement.execute(String sql);
   * statement.getResultSet();
   * </code>.
   */
  protected TajoResultSetBase resultSet = null;

  /**
   * Add SQLWarnings to the warningChain if needed.
   */
  protected SQLWarning warningChain = null;

  /**
   * Keep state so we can fail certain calls made after close().
   */
  private boolean isClosed;

  private boolean blockWait;

  public TajoStatement(JdbcConnection conn, TajoClient tajoClient) {
    this.conn = conn;
    this.tajoClient = tajoClient;
    this.blockWait = tajoClient.getProperties().getBool(SessionVars.BLOCK_ON_RESULT);
  }

  /*
   * NOTICE
   *
   * For unimplemented methods, this class throws an exception or prints an error message.
   * If the unimplemented method can cause unexpected result to user application when it is called,
   * it should throw an exception.
   * Otherwise, it is enough that prints an error message.
   */

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("addBatch() is not supported yet.");
  }

  @Override
  public void cancel() throws SQLException {
    checkConnection("Can't cancel query");
    if (resultSet == null || resultSet.getQueryId().isNull()) {
      return;
    }
    try {
      tajoClient.killQuery(resultSet.getQueryId());
    } catch (Exception e) {
      throw new SQLException(e);
    } finally {
      resultSet = null;
    }
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("clearBatch() is not supported yet.");
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkConnection("Can't clear warnings");
    warningChain = null;
  }

  @Override
  public void close() throws SQLException {
    if (resultSet != null) {
      resultSet.close();
    }
    resultSet = null;
    isClosed = true;
  }

  public void closeOnCompletion() throws SQLException {
     // JDK 1.7
    throw new SQLFeatureNotSupportedException("closeOnCompletion() is not supported yet.");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    resultSet = (TajoResultSetBase) executeQuery(sql);

    return resultSet != null;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("execute() is not supported yet.");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("execute() is not supported yet.");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("execute() is not supported yet.");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("executeBatch() is not supported yet.");
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    checkConnection("Can't execute");

    try {
      return executeSQL(sql);
    } catch (Exception e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  protected ResultSet executeSQL(String sql) throws SQLException, ServiceException, IOException {
    if (isSetVariableQuery(sql)) {
      return setSessionVariable(tajoClient, sql);
    }
    if (isUnSetVariableQuery(sql)) {
      return unSetSessionVariable(tajoClient, sql);
    }

    ClientProtos.SubmitQueryResponse response = tajoClient.executeQuery(sql);
    if (isError(response.getState())) {
      throw new ServiceException(response.getState().getMessage());
    }

    QueryId queryId = new QueryId(response.getQueryId());
    if (response.getIsForwarded() && !queryId.isNull()) {
      WaitingResultSet result = new WaitingResultSet(tajoClient, queryId, fetchSize);
      if (blockWait) {
        result.getSchema();
      }
      return result;
    }

    if (response.hasResultSet() || response.hasTableDesc()) {
      return TajoClientUtil.createResultSet(tajoClient, response, fetchSize);
    }
    return TajoClientUtil.createNullResultSet(queryId);
  }

  protected void checkConnection(String errorMsg) throws SQLException {
    if (isClosed) {
      throw new SQLException(errorMsg + " after statement has been closed");
    }
  }

  public static boolean isSetVariableQuery(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return false;
    }

    return sql.trim().toLowerCase().startsWith("set");
  }

  public static boolean isUnSetVariableQuery(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return false;
    }

    return sql.trim().toLowerCase().startsWith("unset");
  }

  private ResultSet setSessionVariable(TajoClient client, String sql) throws SQLException {
    int index = sql.toLowerCase().indexOf("set");
    if (index < 0) {
      throw new SQLException("SET statement should be started 'SET' keyword: " + sql);
    }

    String[] tokens = sql.substring(index + 3).trim().split(" ");
    if (tokens.length != 2) {
      throw new SQLException("SET statement should be <KEY> <VALUE>: " + sql);
    }
    Map<String, String> variable = new HashMap<String, String>();
    variable.put(tokens[0].trim(), tokens[1].trim());
    try {
      client.updateSessionVariables(variable);
    } catch (ServiceException e) {
      throw new SQLException(e.getMessage(), e);
    }

    return TajoClientUtil.createNullResultSet();
  }

  private ResultSet unSetSessionVariable(TajoClient client, String sql) throws SQLException {
    int index = sql.toLowerCase().indexOf("unset");
    if (index < 0) {
      throw new SQLException("UNSET statement should be started 'UNSET' keyword: " + sql);
    }

    String key = sql.substring(index + 5).trim();
    if (key.isEmpty()) {
      throw new SQLException("UNSET statement should be <KEY>: " + sql);
    }
    try {
      client.unsetSessionVariables(Lists.newArrayList(key));
    } catch (ServiceException e) {
      throw new SQLException(e.getMessage(), e);
    }

    return TajoClientUtil.createNullResultSet();
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    checkConnection("Can't execute update");
    try {
      tajoClient.executeQuery(sql);

      return 1;
    } catch (Exception ex) {
      throw new SQLException(ex.toString());
    }
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("executeUpdate() is not supported yet.");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("executeUpdate() is not supported yet.");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("executeUpdate() is not supported yet.");
  }

  @Override
  public Connection getConnection() throws SQLException {
    checkConnection("Can't get connection");
    return conn;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkConnection("Can't get fetch direction");
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkConnection("Can't get fetch size");
    return fetchSize;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException("getGeneratedKeys() is not supported yet.");
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("getMaxFieldSize() is not supported yet.");
  }

  @Override
  public int getMaxRows() throws SQLException {
    return tajoClient.getMaxRows() ;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new SQLFeatureNotSupportedException("getMoreResults() is not supported yet.");
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLFeatureNotSupportedException("getMoreResults() is not supported yet.");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    System.err.println("getResultSetConcurrency() is not supported yet.");
    return -1;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    checkConnection("Can't get result set");
    return resultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    System.err.println("getResultSetConcurrency() is not supported yet.");
    return -1;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    System.err.println("getResultSetHoldability() is not supported yet.");
    return -1;
  }

  @Override
  public int getResultSetType() throws SQLException {
    System.err.println("getResultSetType() is not supported yet.");
    return -1;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    System.err.println("getResultSetType() is not supported yet.");
    return -1;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkConnection("Can't get warnings");
    return warningChain;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  public boolean isCloseOnCompletion() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("isCloseOnCompletion() is not supported yet.");
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new SQLFeatureNotSupportedException("isPoolable() is not supported yet.");
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    System.err.println("setCursorName() is not supported yet.");
  }

  /**
   * Not necessary.
   */
  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    System.err.println("setEscapeProcessing() is not supported yet.");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    System.err.println("setFetchDirection() is not supported yet.");
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkConnection("Can't set fetch size");
    fetchSize = rows;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    System.err.println("setMaxFieldSize() is not supported yet.");
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
	  if (max < 0) {
	     throw new SQLException("max must be >= 0");
	  }
	  tajoClient.setMaxRows(max);
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    System.err.println("setPoolable() is not supported yet.");
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    System.err.println("setQueryTimeout() is not supported yet.");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    System.err.println("isWrapperFor() is not supported yet.");
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    System.err.println("unwrap() is not supported yet.");
    return null;
  }
}
