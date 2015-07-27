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

import org.apache.tajo.catalog.exception.UndefinedDatabaseException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoRuntimeException;

import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TajoClient {
  private final ClientDelegate delegate;

  /**
   * Initialize TajoClient with a hostname and default port 26002.
   *
   * @param host hostname to connect
   */
  public TajoClient(String host) {
    delegate = ClientDeligateFactory.newDefaultDeligate();
  }

  /**
   * Initialize TajoClient with a hostname and default port 26002.
   *
   * @param host       Hostname to connect
   * @param properties Connection properties
   */
  public TajoClient(String host, Map<String, String> properties) {
    delegate = ClientDeligateFactory.newDefaultDeligate();
  }

  /**
   * Initialize TajoClient with a hostname and port
   *
   * @param host Hostname to connect
   * @param port Port number to connect
   */
  public TajoClient(String host, int port) {
    delegate = ClientDeligateFactory.newDefaultDeligate();
  }

  /**
   * Initialize TajoClient with a hostname and port
   *
   * @param host       Hostname to connect
   * @param port       Port number to connect
   * @param properties Connection properties
   */
  public TajoClient(String host, int port, Map<String, String> properties) {
    delegate = ClientDeligateFactory.newDefaultDeligate();
  }

  /**
   * Initialize TajoClient via service discovery protocol
   *
   * @param discovery Service discovery
   */
  public TajoClient(ServiceDiscovery discovery) {
    delegate = ClientDeligateFactory.newDefaultDeligate();
  }

  /**
   * Initialize TajoClient via service discovery protocol
   *
   * @param discovery Service discovery
   * @param properties Connection properties
   */
  public TajoClient(ServiceDiscovery discovery, Map<String, String> properties) {
    delegate = ClientDeligateFactory.newDefaultDeligate();
  }

  /**
   *
   * @param sql
   * @return
   */
  public QueryHandler executeSQL(String sql) throws TajoException {
    return delegate.executeSQL(sql);
  }

  public ResultSet waitForComplete(QueryHandler handler) throws TajoException {
    try {
      return handler.toFuture().get();
    } catch (TajoRuntimeException e) {
      throw new TajoException(e);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public ResultSet waitForComplete(QueryHandler handler, long duration, TimeUnit unit) throws TimeoutException {
    try {
      return handler.toFuture().get(duration, unit);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public ResultSet executeSQLAndWaitForComplete(String sql) throws TajoException {
    try {
      return waitForComplete(executeSQL(sql));
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public QueryFuture executeSQLAsync(String sql) throws TajoException {
    return null;
  }

  public void selectDB(String database) throws UndefinedDatabaseException {
    delegate.selectDB(database);
  }

  public String currentDB() {
    return delegate.currentDB();
  }
}
