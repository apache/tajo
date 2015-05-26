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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcChannelFactory;
import org.apache.tajo.util.CommonTestingUtil;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class TajoDriver implements Driver, Closeable {
  private static final Log LOG = LogFactory.getLog(TajoDriver.class);
  public static final int MAJOR_VERSION = 1;
  public static final int MINOR_VERSION = 0;

  public static final int JDBC_VERSION_MAJOR = 4;
  public static final int JDBC_VERSION_MINOR = 0;

  public static final String TAJO_JDBC_URL_PREFIX = "jdbc:tajo:";
  private AtomicInteger connections = new AtomicInteger();

  static {
    try {
      DriverManager.registerDriver(new TajoDriver());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public TajoDriver() {
  }

  @Override
  public void close() throws IOException {
    if(connections.decrementAndGet() == 0) {
      if (!System.getProperty(CommonTestingUtil.TAJO_TEST_KEY).equals(CommonTestingUtil.TAJO_TEST_TRUE)) {
        RpcChannelFactory.shutdownGracefully();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Tajo driver is closed");
        }
      }
    }
  }

  @Override
  public Connection connect(String url, Properties properties) throws SQLException {
    connections.incrementAndGet();
    return acceptsURL(url) ? new JdbcConnection(url, properties, this) : null;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(TAJO_JDBC_URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return MINOR_VERSION;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("getParentLogger not supported");
  }
}
