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

import org.apache.tajo.*;
import org.apache.tajo.catalog.CatalogUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetSocketAddress;
import java.sql.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestSQLState extends QueryTestCaseBase {
  private static InetSocketAddress tajoMasterAddress;

  @BeforeClass
  public static void setUp() throws Exception {
    tajoMasterAddress = testingCluster.getMaster().getTajoMasterClientService().getBindAddress();
    Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance();
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  static String buildConnectionUri(String hostName, int port, String databaseName) {
    return "jdbc:tajo://" + hostName + ":" + port + "/" + databaseName;
  }

  private Connection makeConnection() throws SQLException {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    return conn;
  }

  public void assertSQLState(String sql, String sqlState) throws SQLException {
    Connection conn = null;
    Statement stmt = null;
    ResultSet res = null;

    try {
      conn = makeConnection();
      stmt = conn.createStatement();
      res = stmt.executeQuery(sql);
    } catch (SQLException se) {
      assertEquals(sqlState, se.getSQLState());
    } catch (Throwable t) {
      fail(t.getMessage());
    } finally {
      CatalogUtil.closeQuietly(stmt, res);
      CatalogUtil.closeQuietly(conn);
    }
  }

  @Test
  public void testSyntaxError() throws Exception {
    assertSQLState("selec x,y,x from lineitem", "42601");
  }
}
