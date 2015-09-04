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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
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
      assertEquals("Connection refused: localhost/127.0.0.1:" + port, s.getMessage());
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
  public void testImmediateException() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS TestTajoJdbcNegative");
      stmt.executeUpdate("CREATE TABLE TestTajoJdbcNegative.table123u8sd ( name RECORD(last TEXT, first TEXT) )");

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
               stmt.executeQuery("select fail(3, l_orderkey, 'testQueryFailure') from default.lineitem")) {
        fail("Failure must occur here.");
      } catch (SQLException s) {
        assertEquals(toSQLState(ResultCode.INTERNAL_ERROR), s.getSQLState());
      }
    }
  }
}
