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
import org.apache.tajo.*;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.QueryStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.*;

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
        Map<String, Integer> result = Maps.newHashMap();
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
        while (res.next()) {
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
        while (res.next()) {
          assertEquals(resultData[numRows],
            ("" + res.getObject(1).toString() + res.getObject(2).toString() + res.getObject(3).toString()));
          numRows++;
        }
        assertEquals(2, numRows);
      } finally {
        res.close();
      }
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
  public void testResultSetCompression() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    connUri = connUri + "?useCompression=true";
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
        while (res.next()) {
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
        while (res.next()) {
          assertEquals(resultData[numRows],
              ("" + res.getObject(1).toString() + res.getObject(2).toString() + res.getObject(3).toString()));
          numRows++;
        }
        assertEquals(2, numRows);
      } finally {
        res.close();
      }
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

      Set<String> retrivedViaJavaAPI = new HashSet<>(client.getTableList("default"));

      Set<String> retrievedViaJDBC = new HashSet<>();
      while (rs.next()) {
        retrievedViaJDBC.add(rs.getString("TABLE_NAME"));
      }
      assertEquals(retrievedViaJDBC, retrivedViaJavaAPI);
    } finally {
      if (rs != null) {
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

      List<Column> columns = tableDesc.getSchema().getRootColumns();

      while (rs.next()) {
        assertEquals(tableName, rs.getString("TABLE_NAME"));
        assertEquals(columns.get(numColumns).getSimpleName(), rs.getString("COLUMN_NAME"));
        // TODO assert type
        numColumns++;
      }

      assertEquals(16, numColumns);
    } finally {
      if (rs != null) {
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
      for (Connection conn : conns) {
        Statement stmt = null;
        ResultSet res = null;
        try {
          stmt = conn.createStatement();

          res = stmt.executeQuery("select l_returnflag, l_linestatus, count(*) as count_order from lineitem " +
            "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus");

          try {
            Map<String, Integer> result = Maps.newHashMap();
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
          if (res != null) {
            res.close();
          }
          if (stmt != null) {
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
      for (Connection conn : conns) {
        Statement stmt = null;
        ResultSet res = null;
        try {
          stmt = conn.createStatement();

          res = stmt.executeQuery("select l_returnflag, l_linestatus, count(*) as count_order from lineitem " +
            "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus");

          try {
            Map<String, Integer> result = Maps.newHashMap();
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
          if (res != null) {
            res.close();
          }
          if (stmt != null) {
            stmt.close();
          }
          conn.close();
        }
      }
    } finally {
      if (!conns[0].isClosed()) {
        assertTrue(conns[0].isValid(100));
        conns[0].close();
        assertFalse(conns[0].isValid(100));
      }
      if (!conns[1].isClosed()) {
        assertTrue(conns[1].isValid(100));
        conns[1].close();
        assertFalse(conns[1].isValid(100));
      }
    }
  }

  @Test
  public void testCreateTableWithDateAndTimestamp() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testCreateTableWithDateAndTimestamp");

    int result;
    Statement stmt = null;
    ResultSet res = null;
    Connection conn = null;
    try {
      String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
      conn = DriverManager.getConnection(connUri);
      assertTrue(conn.isValid(100));

      stmt = conn.createStatement();
      result = stmt.executeUpdate("create table " + tableName + " (id int, name text, score double"
        + ", register_date timestamp, update_date date, send_date time)");
      assertEquals(result, 1);

      res = stmt.executeQuery("select * from " + tableName);
      assertFalse(res.next());

      ResultSetMetaData rsmd = res.getMetaData();
      assertNotNull(rsmd);
      assertEquals(6, rsmd.getColumnCount());

      assertEquals("id", rsmd.getColumnName(1));
      assertEquals("name", rsmd.getColumnName(2));
      assertEquals("score", rsmd.getColumnName(3));
      assertEquals("register_date", rsmd.getColumnName(4));
      assertEquals("update_date", rsmd.getColumnName(5));
      assertEquals("send_date", rsmd.getColumnName(6));

      assertEquals("integer", rsmd.getColumnTypeName(1));
      assertEquals("varchar", rsmd.getColumnTypeName(2));
      assertEquals("double", rsmd.getColumnTypeName(3));
      assertEquals("timestamp", rsmd.getColumnTypeName(4));
      assertEquals("date", rsmd.getColumnTypeName(5));
      assertEquals("time", rsmd.getColumnTypeName(6));

    } finally {
      cleanupQuery(res);
      if (stmt != null) {
        stmt.close();
      }

      if(conn != null) {
        conn.close();
      }
    }
  }

  @Test
  public void testSortWithDateTime() throws Exception {
    Statement stmt = null;
    ResultSet res = null;
    Connection conn = null;
    int result;

    // skip this test if catalog uses HiveCatalogStore.
    // It is because HiveCatalogStore does not support Time data type.
    try {
      if (!testingCluster.isHiveCatalogStoreRunning()) {
        executeDDL("create_table_with_date_ddl.sql", "table1");

        String connUri = buildConnectionUri(tajoMasterAddress.getHostName(),
          tajoMasterAddress.getPort(), "TestTajoJdbc");

        conn = DriverManager.getConnection(connUri);
        assertTrue(conn.isValid(100));

        stmt = conn.createStatement();
        res = stmt.executeQuery("select col1, col2, col3 from table1 order by col1, col2, col3");

        ResultSetMetaData rsmd = res.getMetaData();
        assertNotNull(rsmd);
        assertEquals(3, rsmd.getColumnCount());

        assertEquals("timestamp", rsmd.getColumnTypeName(1));
        assertEquals("date", rsmd.getColumnTypeName(2));
        assertEquals("time", rsmd.getColumnTypeName(3));

        assertResultSet(res);

        result = stmt.executeUpdate("drop table table1");
        assertEquals(result, 1);

      }
    } finally {
      cleanupQuery(res);
      if (stmt != null) {
        stmt.close();
      }

      if(conn != null) {
        conn.close();
      }
    }
  }

  // TODO: This should be added at TAJO-1891
  public void testAlterTableAddPartition() throws Exception {
    Statement stmt = null;
    ResultSet resultSet = null;
    int retCode = 0;
    Connection conn = null;
    int result;
    String errorMessage = null;

    // skip this test if catalog uses HiveCatalogStore.
    // It is because HiveCatalogStore does not support Time data type.
    try {
      if (!testingCluster.isHiveCatalogStoreRunning()) {
        String connUri = buildConnectionUri(tajoMasterAddress.getHostName(),
            tajoMasterAddress.getPort(), "TestTajoJdbc");

        conn = DriverManager.getConnection(connUri);
        assertTrue(conn.isValid(100));

        String tableName = CatalogUtil.normalizeIdentifier("testAlterTablePartition");
        resultSet = executeString(
          "create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8) ");
        resultSet.close();

        stmt = conn.createStatement();
        result  = stmt.executeUpdate("alter table " + tableName + " add partition (key = 0.1)");
        assertEquals(result, 1);
     }
    } finally {      
      cleanupQuery(resultSet);
      if (stmt != null) {
        stmt.close();
      }

      if(conn != null) {
        conn.close();
      }
    }
  }

  @Test
  public void testMaxRows() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
      DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);
    assertTrue(conn.isValid(100));
    Statement stmt = null;
    ResultSet res = null;
    //Parameter value setting for test.
    final int maxRowsNum = 3;
    int resultRowsNum = 0, returnMaxRows = 0;
    try {
      stmt = conn.createStatement();
      //set maxRows(3)
      stmt.setMaxRows(maxRowsNum);
      //get MaxRows
      returnMaxRows = stmt.getMaxRows();
      res = stmt.executeQuery("select * from lineitem");
      assertNotNull(res);
      while (res.next()) {
        //Actuality result Rows.
        resultRowsNum++;	
      }
      //The test success, if maxRowsNum and resultRowsNum and returnMaxRows is same.
      assertTrue(maxRowsNum == resultRowsNum && maxRowsNum == returnMaxRows);
    } finally {
      if (res != null) {
        cleanupQuery(res);
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
  public final void testCancel() throws Exception {
    String connUri = buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        DEFAULT_DATABASE_NAME);
    Properties props = new Properties();
    props.setProperty(SessionVars.BLOCK_ON_RESULT.keyname(), "false");

    Connection conn = new JdbcConnection(connUri, props);
    PreparedStatement statement = conn.prepareStatement("select sleep(1) from lineitem where l_orderkey > 0");
    try {
      assertTrue("should have result set", statement.execute());
      TajoResultSetBase result = (TajoResultSetBase) statement.getResultSet();
      statement.cancel();
      Thread.sleep(1000);
      QueryStatus status = client.getQueryStatus(result.getQueryId());
      assertEquals(TajoProtos.QueryState.QUERY_KILLED, status.getState());
    } finally {
      if (statement != null) {
        statement.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }
}
