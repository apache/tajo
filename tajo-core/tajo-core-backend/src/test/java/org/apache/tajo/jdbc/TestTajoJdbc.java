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
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
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
  private static TpchTestBase tpch;

  private static InetSocketAddress tajoMasterAddress;
  @BeforeClass
  public static void setUp() throws Exception {
    tajoMasterAddress = testingCluster.getMaster().getTajoMasterClientService().getBindAddress();
    Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance();
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  private static String buildConnectionUri(String hostName, int port, String databaseNme) {
    return "jdbc:tajo://" + hostName + ":" + port + "/" + databaseNme;
  }

  @Test
  public void testStatement() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);

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
    DatabaseMetaData dbmd = conn.getMetaData();

    ResultSet rs = null;

    try {
      rs = dbmd.getTables(null, null, null, null);

      ResultSetMetaData rsmd = rs.getMetaData();
      int numCols = rsmd.getColumnCount();
      assertEquals(5, numCols);

      Set<String> retrivedViaJavaAPI = new HashSet<String>(client.getTableList(DEFAULT_DATABASE_NAME));

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
  }

  @Test
  public void testDatabaseMetaDataGetColumns() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
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
        //TODO assert type
        numColumns++;
      }

      assertEquals(16, numColumns);
    } finally {
      if(rs != null) {
        rs.close();
      }
    }
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
      conns[0].close();
      conns[1].close();
    }
  }

  @Test
  public void testMultipleConnectionsSequentialClose() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(), DEFAULT_DATABASE_NAME);

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
        conns[0].close();
      }
      if(!conns[1].isClosed()) {
        conns[1].close();
      }
    }
  }

  @Test
  public void testSetAndGetCatalog() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);

    assertDatabaseNotExists("jdbc_test1");
    PreparedStatement pstmt = conn.prepareStatement("CREATE DATABASE jdbc_test1");
    pstmt.executeUpdate();
    assertDatabaseExists("jdbc_test1");
    pstmt.close();

    pstmt = conn.prepareStatement("CREATE DATABASE jdbc_test2");
    pstmt.executeUpdate();
    assertDatabaseExists("jdbc_test2");
    pstmt.close();

    conn.setCatalog("jdbc_test1");
    assertEquals("jdbc_test1", conn.getCatalog());
    conn.setCatalog("jdbc_test2");
    assertEquals("jdbc_test2", conn.getCatalog());
    conn.setCatalog("jdbc_test1");
    assertEquals("jdbc_test1", conn.getCatalog());

    conn.setCatalog(TajoConstants.DEFAULT_DATABASE_NAME);
    pstmt = conn.prepareStatement("DROP DATABASE jdbc_test1");
    pstmt.executeUpdate();
    pstmt.close();
    pstmt = conn.prepareStatement("DROP DATABASE jdbc_test2");
    pstmt.executeUpdate();
    pstmt.close();

    conn.close();
  }

  @Test
  public void testGetCatalogsAndTables() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection defaultConnect = DriverManager.getConnection(connUri);

    Set<String> existingDatabases = new HashSet<String>();
    DatabaseMetaData dbmd = defaultConnect.getMetaData();
    ResultSet res = dbmd.getCatalogs();
    while(res.next()) {
      existingDatabases.add(res.getString(1));
    }
    res.close();

    // create database "jdbc_test1" and its tables
    assertDatabaseNotExists("jdbc_test1");
    PreparedStatement pstmt = defaultConnect.prepareStatement("CREATE DATABASE jdbc_test1");
    pstmt.executeUpdate();
    assertDatabaseExists("jdbc_test1");
    pstmt.close();
    pstmt = defaultConnect.prepareStatement("CREATE TABLE jdbc_test1.table1 (age int)");
    pstmt.executeUpdate();
    pstmt.close();
    pstmt = defaultConnect.prepareStatement("CREATE TABLE jdbc_test1.table2 (age int)");
    pstmt.executeUpdate();
    pstmt.close();

    // create database "jdbc_test2" and its tables
    pstmt = defaultConnect.prepareStatement("CREATE DATABASE jdbc_test2");
    pstmt.executeUpdate();
    assertDatabaseExists("jdbc_test2");
    pstmt.close();

    pstmt = defaultConnect.prepareStatement("CREATE TABLE jdbc_test2.table3 (age int)");
    pstmt.executeUpdate();
    pstmt.close();
    pstmt = defaultConnect.prepareStatement("CREATE TABLE jdbc_test2.table4 (age int)");
    pstmt.executeUpdate();
    pstmt.close();

    // verify getCatalogs()
    Set<String> newDatabases = new HashSet<String>();
    dbmd = defaultConnect.getMetaData();
    res = dbmd.getCatalogs();
    while(res.next()) {
      newDatabases.add(res.getString(1));
    }
    res.close();
    newDatabases.removeAll(existingDatabases);
    assertEquals(2, newDatabases.size());
    assertTrue(newDatabases.contains("jdbc_test1"));
    assertTrue(newDatabases.contains("jdbc_test2"));

    // verify getTables()
    res = defaultConnect.getMetaData().getTables("jdbc_test1", null, null, null);
    assertResultSet(res, "getTables1.result");
    res.close();
    res = defaultConnect.getMetaData().getTables("jdbc_test2", null, null, null);
    assertResultSet(res, "getTables2.result");
    res.close();

    defaultConnect.close();

    // jdbc1_test database connection test
    String jdbcTest1ConnUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        "jdbc_test1");
    Connection jdbcTest1Conn = DriverManager.getConnection(jdbcTest1ConnUri);
    assertEquals("jdbc_test1", jdbcTest1Conn.getCatalog());
    jdbcTest1Conn.close();

    String jdbcTest2ConnUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        "jdbc_test2");
    Connection jdbcTest2Conn = DriverManager.getConnection(jdbcTest2ConnUri);
    assertEquals("jdbc_test2", jdbcTest2Conn.getCatalog());
    jdbcTest2Conn.close();

    executeString("DROP DATABASE jdbc_test1");
    executeString("DROP DATABASE jdbc_test2");
  }
}