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

import com.google.common.collect.Maps;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.TajoClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestTajoJdbc extends QueryTestCaseBase {
  private static InetSocketAddress tajoMasterAddress;

  @BeforeClass
  public static void setUp() throws Exception {
    tajoMasterAddress = testingCluster.getMaster().getTajoMasterClientService().getBindAddress();
    Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance();
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  public static String buildConnectionUri(String hostName, int port, String databaseName) {
    return "jdbc:tajo://" + hostName + ":" + port + "/" + databaseName;
  }

  @Test
  public void testAcceptURL() throws SQLException {
    TajoDriver driver = new TajoDriver();
    assertTrue(driver.acceptsURL("jdbc:tajo:"));
    assertFalse(driver.acceptsURL("jdbc:taju:"));
  }

  @Test(expected = SQLException.class)
  public void testGetConnection() throws SQLException {
    DriverManager.getConnection("jdbc:taju://" + tajoMasterAddress.getHostName() + ":" + tajoMasterAddress.getPort()
        + "/default");
  }

  @Test
  public void testStatement() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    Statement stmt = null;
    ResultSet res = null;
    try {
      stmt = conn.createStatement();

      res = stmt.executeQuery("select l_returnflag, l_linestatus, count(*) as count_order from lineitem " +
          "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus");

      try {
        Map<String,Integer> result = Maps.newHashMap();
        result.put("NO", 3);
        result.put("RF", 2);

        assertNotNull(res);
        assertTrue(res.next());
        assertTrue(result.get(res.getString(1) + res.getString(2)) == res.getInt(3));
        assertTrue(res.next());
        assertTrue(result.get(res.getString(1) + res.getString(2)) == res.getInt(3));
        assertFalse(res.next());

        ResultSetMetaData rsmd = res.getMetaData();
        assertEquals(3, rsmd.getColumnCount());
        assertEquals("l_returnflag", rsmd.getColumnName(1));
        assertEquals("l_linestatus", rsmd.getColumnName(2));
        assertEquals("count_order", rsmd.getColumnName(3));
      } finally {
        res.close();
      }
    } finally {
      if(res != null) {
        res.close();
      }
      if(stmt != null) {
        stmt.close();
      }
    }
  }

  @Test
  public void testPreparedStatement() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    PreparedStatement stmt = null;
    ResultSet res = null;
    try {
      /*
      test data set
      1,17.0,N
      1,36.0,N
      2,38.0,N
      3,45.0,R
      3,49.0,R
      */

      String sql =
          "select l_orderkey, l_quantity, l_returnflag from lineitem where l_quantity > ? and l_returnflag = ?";

      stmt = conn.prepareStatement(sql);

      stmt.setInt(1, 20);
      stmt.setString(2, "N");

      res = stmt.executeQuery();

      ResultSetMetaData rsmd = res.getMetaData();
      assertEquals(3, rsmd.getColumnCount());
      assertEquals("l_orderkey", rsmd.getColumnName(1));
      assertEquals("l_quantity", rsmd.getColumnName(2));
      assertEquals("l_returnflag", rsmd.getColumnName(3));

      try {
        int numRows = 0;
        String[] resultData = {"136.0N", "238.0N"};
        while(res.next()) {
          assertEquals(resultData[numRows],
              ("" + res.getObject(1).toString() + res.getObject(2).toString() + res.getObject(3).toString()));
          numRows++;
        }
        assertEquals(2, numRows);
      } finally {
        res.close();
      }

      stmt.setInt(1, 20);
      stmt.setString(2, "R");

      res = stmt.executeQuery();

      rsmd = res.getMetaData();
      assertEquals(3, rsmd.getColumnCount());
      assertEquals("l_orderkey", rsmd.getColumnName(1));
      assertEquals("l_quantity", rsmd.getColumnName(2));
      assertEquals("l_returnflag", rsmd.getColumnName(3));

      try {
        int numRows = 0;
        String[] resultData = {"345.0R", "349.0R"};
        while(res.next()) {
          assertEquals(resultData[numRows],
              ("" + res.getObject(1).toString() + res.getObject(2).toString() + res.getObject(3).toString()));
          numRows++;
        }
        assertEquals(2, numRows);
      } finally {
        res.close();
      }
    } finally {
      if(res != null) {
        res.close();
      }
      if(stmt != null) {
        stmt.close();
      }
    }
  }

  @Test
  public void testDatabaseMetaDataGetTable() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    DatabaseMetaData dbmd = conn.getMetaData();

    ResultSet rs = null;

    try {
      rs = dbmd.getTables("default", null, null, null);

      ResultSetMetaData rsmd = rs.getMetaData();
      int numCols = rsmd.getColumnCount();
      assertEquals(5, numCols);

      Set<String> retrivedViaJavaAPI = new HashSet<String>(client.getTableList("default"));

      Set<String> retrievedViaJDBC = new HashSet<String>();
      while(rs.next()) {
        retrievedViaJDBC.add(rs.getString("TABLE_NAME"));
      }
      assertEquals(retrievedViaJDBC, retrivedViaJavaAPI);
    } finally {
      if(rs != null) {
        rs.close();
      }
    }

    assertTrue(conn.isValid(100));
    conn.close();
  }

  @Test
  public void testDatabaseMetaDataGetColumns() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));

    DatabaseMetaData dbmd = conn.getMetaData();
    ResultSet rs = null;

    try {
      String tableName = "lineitem";
      rs = dbmd.getColumns(null, null, tableName, null);

      ResultSetMetaData rsmd = rs.getMetaData();
      int numCols = rsmd.getColumnCount();

      assertEquals(22, numCols);
      int numColumns = 0;

      TableDesc tableDesc = client.getTableDesc(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, tableName));
      assertNotNull(tableDesc);

      List<Column> columns = tableDesc.getSchema().getColumns();

      while(rs.next()) {
        assertEquals(tableName, rs.getString("TABLE_NAME"));
        assertEquals(columns.get(numColumns).getSimpleName(), rs.getString("COLUMN_NAME"));
        // TODO assert type
        numColumns++;
      }

      assertEquals(16, numColumns);
    } finally {
      if(rs != null) {
        rs.close();
      }
    }

    assertTrue(conn.isValid(100));
    conn.close();
    assertFalse(conn.isValid(100));
  }

  @Test
  public void testMultipleConnections() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);

    Connection[] conns = new Connection[2];
    conns[0] = DriverManager.getConnection(connUri);
    conns[1] = DriverManager.getConnection(connUri);

    try {
      for(int i = 0; i < conns.length; i++) {
        Statement stmt = null;
        ResultSet res = null;
        try {
          stmt = conns[i].createStatement();

          res = stmt.executeQuery("select l_returnflag, l_linestatus, count(*) as count_order from lineitem " +
              "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus");

          try {
            Map<String,Integer> result = Maps.newHashMap();
            result.put("NO", 3);
            result.put("RF", 2);

            assertNotNull(res);
            assertTrue(res.next());
            assertTrue(result.get(res.getString(1) + res.getString(2)) == res.getInt(3));
            assertTrue(res.next());
            assertTrue(result.get(res.getString(1) + res.getString(2)) == res.getInt(3));
            assertFalse(res.next());

            ResultSetMetaData rsmd = res.getMetaData();
            assertEquals(3, rsmd.getColumnCount());
            assertEquals("l_returnflag", rsmd.getColumnName(1));
            assertEquals("l_linestatus", rsmd.getColumnName(2));
            assertEquals("count_order", rsmd.getColumnName(3));
          } finally {
            res.close();
          }
        } finally {
          if(res != null) {
            res.close();
          }
          if(stmt != null) {
            stmt.close();
          }
        }
      }
    } finally {
      assertTrue(conns[0].isValid(100));
      conns[0].close();
      assertFalse(conns[0].isValid(100));
      assertTrue(conns[1].isValid(100));
      conns[1].close();
      assertFalse(conns[1].isValid(100));
    }
  }

  @Test
  public void testMultipleConnectionsSequentialClose() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);

    Connection[] conns = new Connection[2];
    conns[0] = DriverManager.getConnection(connUri);
    conns[1] = DriverManager.getConnection(connUri);

    try {
      for(int i = 0; i < conns.length; i++) {
        Statement stmt = null;
        ResultSet res = null;
        try {
          stmt = conns[i].createStatement();

          res = stmt.executeQuery("select l_returnflag, l_linestatus, count(*) as count_order from lineitem " +
              "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus");

          try {
            Map<String,Integer> result = Maps.newHashMap();
            result.put("NO", 3);
            result.put("RF", 2);

            assertNotNull(res);
            assertTrue(res.next());
            assertTrue(result.get(res.getString(1) + res.getString(2)) == res.getInt(3));
            assertTrue(res.next());
            assertTrue(result.get(res.getString(1) + res.getString(2)) == res.getInt(3));
            assertFalse(res.next());

            ResultSetMetaData rsmd = res.getMetaData();
            assertEquals(3, rsmd.getColumnCount());
            assertEquals("l_returnflag", rsmd.getColumnName(1));
            assertEquals("l_linestatus", rsmd.getColumnName(2));
            assertEquals("count_order", rsmd.getColumnName(3));
          } finally {
            res.close();
          }
        } finally {
          if(res != null) {
            res.close();
          }
          if(stmt != null) {
            stmt.close();
          }
          conns[i].close();
        }
      }
    } finally {
      if(!conns[0].isClosed()) {
        assertTrue(conns[0].isValid(100));
        conns[0].close();
        assertFalse(conns[0].isValid(100));
      }
      if(!conns[1].isClosed()) {
        assertTrue(conns[1].isValid(100));
        conns[1].close();
        assertFalse(conns[1].isValid(100));
      }
    }
  }

  @Test
  public void testSetStatement() throws Exception {
    assertTrue(TajoStatement.isSetVariableQuery("Set JOIN_TASK_INPUT_SIZE 123"));
    assertTrue(TajoStatement.isSetVariableQuery("SET JOIN_TASK_INPUT_SIZE 123"));
    assertFalse(TajoStatement.isSetVariableQuery("--SET JOIN_TASK_INPUT_SIZE 123"));

    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);

    Connection conn = DriverManager.getConnection(connUri);

    Statement stmt = null;
    ResultSet res = null;
    try {
      stmt = conn.createStatement();
      res = stmt.executeQuery("Set JOIN_TASK_INPUT_SIZE 123");
      assertFalse(res.next());
      ResultSetMetaData rsmd = res.getMetaData();
      assertNotNull(rsmd);
      assertEquals(0, rsmd.getColumnCount());

      TajoClient connTajoClient = ((TajoConnection)stmt.getConnection()).getTajoClient();
      Map<String, String> variables = connTajoClient.getAllSessionVariables();
      String value = variables.get("JOIN_TASK_INPUT_SIZE");
      assertNotNull(value);
      assertEquals("123", value);

      res.close();

      res = stmt.executeQuery("unset JOIN_TASK_INPUT_SIZE");
      variables = connTajoClient.getAllSessionVariables();
      value = variables.get("JOIN_TASK_INPUT_SIZE");
      assertNull(value);
    } finally {
      if (res != null) {
        res.close();
      }
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }

  @Test
  public void testSetPreparedStatement() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);

    Connection conn = DriverManager.getConnection(connUri);

    PreparedStatement stmt = null;
    ResultSet res = null;
    try {
      stmt = conn.prepareStatement("Set JOIN_TASK_INPUT_SIZE 123");
      res = stmt.executeQuery();
      assertFalse(res.next());
      ResultSetMetaData rsmd = res.getMetaData();
      assertNotNull(rsmd);
      assertEquals(0, rsmd.getColumnCount());

      TajoClient connTajoClient = ((TajoConnection)stmt.getConnection()).getTajoClient();
      Map<String, String> variables = connTajoClient.getAllSessionVariables();
      String value = variables.get("JOIN_TASK_INPUT_SIZE");
      assertNotNull(value);
      assertEquals("123", value);

      res.close();
      stmt.close();

      stmt = conn.prepareStatement("unset JOIN_TASK_INPUT_SIZE");
      res = stmt.executeQuery();
      variables = connTajoClient.getAllSessionVariables();
      value = variables.get("JOIN_TASK_INPUT_SIZE");
      assertNull(value);
    } finally {
      if (res != null) {
        res.close();
      }
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }
}