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

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class TajoConnection implements Connection {
  private static Log LOG = LogFactory.getLog(TajoConnection.class);

  private final TajoClient tajoClient;
  private final AtomicBoolean closed = new AtomicBoolean(true);
  private final String rawURI;
  private final Properties properties;

  private final URI uri;
  private final String hostName;
  private final int port;
  private final String databaseName;
  @SuppressWarnings("unused")
  /** it will be used soon. */
  private final Map<String, List<String>> params;

  public TajoConnection(String rawURI, Properties properties) throws SQLException {
    this.rawURI = rawURI;
    this.properties = properties;

    try {
      if (!rawURI.startsWith(TajoDriver.TAJO_JDBC_URL_PREFIX)) {
        throw new SQLException("Invalid URL: " + rawURI, "TAJO-001");
      }

      // URI form: jdbc:tajo://hostname:port/databasename
      int startIdx = rawURI.indexOf(":");
      if (startIdx < 0) {
        throw new SQLException("Invalid URL: " + rawURI, "TAJO-001");
      }

      String uri = rawURI.substring(startIdx+1, rawURI.length());
      try {
        this.uri = URI.create(uri);
      } catch (IllegalArgumentException iae) {
        throw new SQLException("Invalid URL: " + rawURI, "TAJO-001");
      }

      hostName = this.uri.getHost();
      if(hostName == null) {
        throw new SQLException("Invalid JDBC URI: " + rawURI, "TAJO-001");
      }
      if (this.uri.getPort() < 1) {
        port = 26002;
      } else {
        port = this.uri.getPort();
      }

      if (this.uri.getPath() == null || this.uri.getPath().equalsIgnoreCase("")) { // if no database is given, set default.
        databaseName = TajoConstants.DEFAULT_DATABASE_NAME;
      } else {
        // getPath() will return '/database'.
        databaseName = this.uri.getPath().split("/")[1];
      }

      params = new QueryStringDecoder(rawURI).getParameters();
    } catch (SQLException se) {
      throw se;
    } catch (Throwable t) { // for unexpected exceptions like ArrayIndexOutOfBoundsException.
      throw new SQLException("Invalid JDBC URI: " + rawURI, "TAJO-001");
    }

    TajoConf tajoConf = new TajoConf();
    if(properties != null) {
      for(Map.Entry<Object, Object> entry: properties.entrySet()) {
        tajoConf.set(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    try {
      tajoClient = new TajoClient(hostName, port, databaseName);
    } catch (Exception e) {
      throw new SQLException("Cannot create TajoClient instance:" + e.getMessage(), "TAJO-002");
    }
    closed.set(false);
  }

  public String getUri() {
    return this.rawURI;
  }

  public TajoClient getTajoClient() {
    return tajoClient;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public void close() throws SQLException {
    if(!closed.get()) {
      if(tajoClient != null) {
        tajoClient.close();
      }

      closed.set(true);
    }
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLFeatureNotSupportedException("commit");
  }

  @Override
  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    throw new SQLFeatureNotSupportedException("createArrayOf");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createBlob");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createClob");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createNClob");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException("createSQLXML");
  }

  @Override
  public Statement createStatement() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    return new TajoStatement(this, tajoClient);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("createStatement");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                   int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException("createStatement");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("createStruct");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public String getCatalog() throws SQLException {
    try {
      return tajoClient.getCurrentDatabase();
    } catch (ServiceException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLFeatureNotSupportedException("getClientInfo");
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getClientInfo");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("getHoldability");
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new TajoDatabaseMetaData(this);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException("getTypeMap");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException("getWarnings");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed.get();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    try {
      if (tajoClient.isConnected()) {
        ResultSet resultSet = tajoClient.executeQueryAndGetResult("SELECT 1;");
        boolean next = resultSet.next();
        boolean valid = next && resultSet.getLong(1) == 1;
        resultSet.close();
        return valid;
      } else {
        return false;
      }
    } catch (ServiceException e) {
      LOG.error("TajoMaster is not available.");
      return false;
    } catch (IOException e) {
      LOG.error("JDBC connection is not valid.");
      return false;
    }
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("nativeSQL");
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareCall");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency) throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareCall");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareCall");
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new TajoPreparedStatement(this, tajoClient, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    return new TajoPreparedStatement(this, tajoClient, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareStatement");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareStatement");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency) throws SQLException {
    return new TajoPreparedStatement(this, tajoClient, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareStatement");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("releaseSavepoint");
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException("rollback");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("rollback");
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throw new SQLFeatureNotSupportedException("setAutoCommit");
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    try {
      tajoClient.selectDatabase(catalog);
    } catch (ServiceException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    throw new UnsupportedOperationException("setClientInfo");
  }

  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    throw new UnsupportedOperationException("setClientInfo");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new SQLFeatureNotSupportedException("setHoldability");
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new SQLFeatureNotSupportedException("setReadOnly");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException("setSavepoint");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("setSavepoint");
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new SQLFeatureNotSupportedException("setTransactionIsolation");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("setTypeMap");
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    if (isWrapperFor(tClass)) {
      return (T) this;
    }
    throw new SQLException("No wrapper for " + tClass);
  }

  @Override
  public boolean isWrapperFor(Class<?> tClass) throws SQLException {
    return tClass.isInstance(this);
  }

  public void abort(Executor executor) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("abort is not supported");
  }

  public int getNetworkTimeout() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("getNetworkTimeout is not supported");
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("setNetworkTimeout not supported");
  }

  public String getSchema() throws SQLException {
    return TajoConstants.DEFAULT_SCHEMA_NAME;
  }

  public void setSchema(String schema) throws SQLException {
    throw new SQLFeatureNotSupportedException("setSchema() is not supported yet");
  }
}
