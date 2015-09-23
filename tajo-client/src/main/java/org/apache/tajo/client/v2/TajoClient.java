/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.client.v2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.exception.QueryFailedException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.client.v2.exception.ClientUnableToConnectException;
import org.apache.tajo.exception.TajoException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

public class TajoClient implements Closeable {
  private static Log LOG = LogFactory.getLog(TajoClient.class);

  /**
   * default client port number
   */
  public static final int DEFAULT_PORT = 26002;

  private final ClientDelegate delegate;

  /**
   * Initialize TajoClient with a hostname and default port 26002.
   *
   * @param host hostname to connect
   */
  public TajoClient(String host) throws ClientUnableToConnectException {
    delegate = ClientDelegateFactory.newDefaultDelegate(host, DEFAULT_PORT, null);
  }

  /**
   * Initialize TajoClient with a hostname and default port 26002.
   *
   * @param host         Hostname to connect
   * @param clientParams Client connection parameters
   */
  public TajoClient(String host, Properties clientParams) throws ClientUnableToConnectException {
    delegate = ClientDelegateFactory.newDefaultDelegate(host, DEFAULT_PORT, clientParams);
  }

  /**
   * Initialize TajoClient with a hostname and port
   *
   * @param host Hostname to connect
   * @param port Port number to connect
   */
  public TajoClient(String host, int port) throws ClientUnableToConnectException {
    delegate = ClientDelegateFactory.newDefaultDelegate(host, port, null);
  }

  /**
   * Initialize TajoClient with a hostname and port
   *
   * @param host           Hostname to connect
   * @param port           Port number to connect
   * @param clientParams   Client connection parameters
   */
  public TajoClient(String host, int port, Properties clientParams) throws ClientUnableToConnectException {
    delegate = ClientDelegateFactory.newDefaultDelegate(host, port, clientParams);
  }

  /**
   * Initialize TajoClient via service discovery protocol
   *
   * @param discovery Service discovery
   */
  public TajoClient(ServiceDiscovery discovery) throws ClientUnableToConnectException {
    delegate = ClientDelegateFactory.newDefaultDelegate(discovery, null);
  }

  /**
   * Initialize TajoClient via service discovery protocol
   *
   * @param discovery    Service discovery
   * @param clientParams Client connection parameters
   */
  public TajoClient(ServiceDiscovery discovery, Properties clientParams) throws ClientUnableToConnectException {
    delegate = ClientDelegateFactory.newDefaultDelegate(discovery, clientParams);
  }

  /**
   * Submit and executes the given SQL statement, which may be an <code>INSERT (INTO)</code>,
   * or <code>CREATE TABLE AS SELECT</code> statement or anSQL statement that returns nothing,
   * such as an SQL DDL statement.
   *
   * @param sql a SQL statement
   * @return inserted row number
   * @throws TajoException
   */
  public int executeUpdate(String sql) throws TajoException {
    return delegate.executeUpdate(sql);
  }

  /**
   * Submit a SQL query statement
   *
   * @param sql a SQL statement
   * @return QueryHandler
   * @throws TajoException
   */
  public ResultSet executeQuery(String sql) throws TajoException, QueryFailedException {
    return delegate.executeSQL(sql);
  }

  /**
   * Execute a SQL statement through asynchronous API
   *
   * @param sql
   * @return
   * @throws TajoException
   */
  public QueryFuture executeQueryAsync(String sql) throws TajoException {
    return delegate.executeSQLAsync(sql);
  }

  public void close() {
    try {
      delegate.close();
    } catch (IOException e) {
      LOG.warn(e);
    }
  }

  /**
   * Select working database
   *
   * @param database Database name
   * @throws UndefinedDatabaseException
   */
  public void selectDB(String database) throws UndefinedDatabaseException {
    delegate.selectDB(database);
  }

  /**
   * Get the current working database
   *
   * @return Current working database
   */
  public String currentDB() {
    return delegate.currentDB();
  }
}
