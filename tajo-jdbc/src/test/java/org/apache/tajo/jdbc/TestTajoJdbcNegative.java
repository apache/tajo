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

package org.apache.tajo.jdbc;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.exception.SQLExceptionUtil;
import org.apache.tajo.util.UriUtil;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.*;
import java.util.Properties;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.error.Errors.ResultCode.CLIENT_CONNECTION_EXCEPTION;
import static org.apache.tajo.exception.SQLExceptionUtil.toSQLState;
import static org.apache.tajo.jdbc.TestTajoJdbc.buildConnectionUri;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestTajoJdbcNegative extends QueryTestCaseBase {
  private static InetSocketAddress tajoMasterAddress;

  @BeforeClass
  public static void setUp() throws Exception {
    tajoMasterAddress = testingCluster.getMaster().getTajoMasterClientService().getBindAddress();
    Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance();
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  @Test(expected = SQLException.class)
  public void testGetConnection() throws SQLException {
    DriverManager.getConnection("jdbc:taju://" + tajoMasterAddress.getHostName() + ":" + tajoMasterAddress.getPort()
        + "/default");
  }

  @Test
  public void testUnresolvedError() throws SQLException {
    try {
      DriverManager.getConnection("jdbc:tajo://tajo-unknown-asdnkl213.asd:2002/default");
    } catch (SQLException s) {
      assertEquals(toSQLState(ResultCode.CLIENT_CONNECTION_EXCEPTION), s.getSQLState());
      assertEquals("Can't resolve host name: tajo-unknown-asdnkl213.asd:2002", s.getMessage());
    }
  }

  @Test
  public void testConnectionRefused() throws SQLException, IOException {
    Integer port = null;
    try {
      ServerSocket s = new ServerSocket(0);
      port = s.getLocalPort();
      s.close();
      DriverManager.getConnection("jdbc:tajo://localhost:" + port + "/default");
      fail("Must be failed.");
    } catch (SQLException s) {
      assertEquals(toSQLState(ResultCode.CLIENT_CONNECTION_EXCEPTION), s.getSQLState());
    }
  }

  @Test
  public void testConnectionClosedAtCreateStmt() throws SQLException, IOException {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    conn.close();
    try (Statement stmt = conn.createStatement()) {
      fail("Must be failed.");
      stmt.isClosed();
    } catch (SQLException s) {
      assertEquals(toSQLState(ResultCode.CLIENT_CONNECTION_DOES_NOT_EXIST), s.getSQLState());
      assertEquals("This connection has been closed.", s.getMessage());
    }
  }

  @Test
  public void testConnectionClosed() throws SQLException, IOException {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    try (Statement stmt = conn.createStatement()) {
      conn.close();
      stmt.executeUpdate("SELECT 1;");
      fail("Must be failed.");
    } catch (SQLException s) {
      assertEquals(toSQLState(ResultCode.CLIENT_CONNECTION_DOES_NOT_EXIST), s.getSQLState());
      assertEquals("This connection has been closed.", s.getMessage());
    }
  }

  @Test
  public void testSyntaxErrorOnExecuteUpdate() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate("CREATE TABLE \n1table123u8sd ( name RECORD(last TEXT, first TEXT) )");
      fail("Must be failed");
    } catch (SQLException s) {
      assertEquals(toSQLState(ResultCode.SYNTAX_ERROR), s.getSQLState());
      assertEquals(
          "ERROR: syntax error at or near \"1\"\n" +
          "LINE 2: 1table123u8sd ( name RECORD(last TEXT, first TEXT) )\n" +
          "        ^", s.getMessage());
    }
  }

  @Test
  public void testSyntaxErrorOnExecuteQuery() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    try (Statement stmt = conn.createStatement()) {
      try (ResultSet result = stmt.executeQuery("SELECT\n*\nFROM_ LINEITEM")) {
        fail("Must be failed");
      } catch (SQLException s) {
        assertEquals(toSQLState(ResultCode.SYNTAX_ERROR), s.getSQLState());
        assertEquals(
            "ERROR: syntax error at or near \"from_\"\n" +
            "LINE 3: FROM_ LINEITEM\n" +
            "        ^^^^^", s.getMessage());
      }
    }
  }

  @Test
  public void testImmediateException() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS TestTajoJdbcNegative");
      stmt.executeUpdate("CREATE TABLE TestTajoJdbcNegative.table123u8sd ( name RECORD (last TEXT, first TEXT) )");

      try (ResultSet resultSet = stmt.executeQuery("select name FROM TestTajoJdbcNegative.table123u8sd")) {
        fail("Getting a record type field must be failed");
      } catch (SQLException s) {
        assertEquals(toSQLState(ResultCode.NOT_IMPLEMENTED), s.getSQLState());
      } finally {
        stmt.executeUpdate("DROP TABLE IF EXISTS TestTajoJdbcNegative.table12u79");
        stmt.executeUpdate("DROP DATABASE IF EXISTS TestTajoJdbcNegative");
      }
    }
  }

  @Test
  public void testExceptionDuringProcessing() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    try (Statement stmt = conn.createStatement()) {
      try (ResultSet resultSet =
               stmt.executeQuery(
                   "select fail(3, l_orderkey, 'testQueryFailure') from default.lineitem where l_orderkey > 0")) {
        fail("Failure must occur here.");
      } catch (SQLException s) {
        assertEquals(toSQLState(ResultCode.INTERNAL_ERROR), s.getSQLState());
      }
    }
  }

  private void assumeConnectTimeout(String host, int port, int connectTimeout) throws IOException {
    try (Socket socket = new Socket())  {
      // Try to connect to a private address in the 10.x.y.z range.
      // These addresses are usually not routed, so an attempt to
      // connect to them will hang the connection attempt, which is
      // what we want to simulate in this test.
      socket.connect(new InetSocketAddress(host, port), connectTimeout);
      // Abort the test if we can connect.
      Assume.assumeTrue(false);
    } catch (SocketTimeoutException x) {
      // Expected timeout during connect, continue the test.
      Assume.assumeTrue(true);
    } catch (Throwable x) {
      // Abort if any other exception happens.
      Assume.assumeTrue(false);
    }
  }

  @Test(timeout = 5000)
  public final void testConnectTimeout() throws Exception {
    final String host = "10.255.255.1";
    final int port = 80;
    int connectTimeout = 1000;
    assumeConnectTimeout(host, port, connectTimeout);

    long startTime = Long.MIN_VALUE;
    long endTime;
    try {
      // artificially cause connection timeout
      String connUri = buildConnectionUri(host, port, DEFAULT_DATABASE_NAME);
      connUri = UriUtil.addParam(connUri, "connectTimeout", "1"); // 1 seconds
      connUri = UriUtil.addParam(connUri, "retry", "0"); // 1 seconds
      startTime = System.currentTimeMillis();
      new JdbcConnection(connUri, new Properties());
      fail("Must be failed");
    } catch (SQLException t) {
      endTime = System.currentTimeMillis();
      assertEquals(t.getSQLState(), SQLExceptionUtil.toSQLState(CLIENT_CONNECTION_EXCEPTION));
      assertEquals("connection timed out: /10.255.255.1:80", t.getMessage());
      // default is 15 seconds. So, if timeout is shorter than 1~2 seconds.
      // We can ensure the parameter was effective.
      assertTrue(((endTime - startTime) / 1000) < 2);
    }
  }
}
