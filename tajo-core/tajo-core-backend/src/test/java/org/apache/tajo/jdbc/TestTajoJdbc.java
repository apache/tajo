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
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.NetUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestTajoJdbc {
  private static TpchTestBase tpch;
  private static Connection conn;

  private static String connUri;
  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();

    TajoConf tajoConf = tpch.getTestingCluster().getMaster().getContext().getConf();
    InetSocketAddress tajoMasterAddress =
        NetUtils.createSocketAddr(tajoConf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS));

    Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance();

    connUri = "jdbc:tajo://" + tajoMasterAddress.getHostName() + ":" + tajoMasterAddress.getPort();
    conn = DriverManager.getConnection(connUri);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(conn != null) {
      conn.close();
    }
  }

  @Test
  public void testStatement() throws Exception {
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
    DatabaseMetaData dbmd = conn.getMetaData();

    ResultSet rs = null;

    try {
      rs = dbmd.getTables(null, null, null, null);

      ResultSetMetaData rsmd = rs.getMetaData();
      int numCols = rsmd.getColumnCount();

      assertEquals(5, numCols);
      int numTables = 0;

      List<String> tableNames = new ArrayList<String>(
          tpch.getTestingCluster().getMaster().getCatalog().getAllTableNames());

      Collections.sort(tableNames);

      while(rs.next()) {
        assertEquals(tableNames.get(numTables), rs.getString("TABLE_NAME"));
        numTables++;
      }

      assertEquals(tableNames.size(), numTables);
    } finally {
      if(rs != null) {
        rs.close();
      }
    }
  }

  @Test
  public void testDatabaseMetaDataGetColumns() throws Exception {
    DatabaseMetaData dbmd = conn.getMetaData();

    ResultSet rs = null;

    try {
      String tableName = "lineitem";
      rs = dbmd.getColumns(null, null, tableName, null);

      ResultSetMetaData rsmd = rs.getMetaData();
      int numCols = rsmd.getColumnCount();

      assertEquals(22, numCols);
      int numColumns = 0;

      TableDesc tableDesc = tpch.getTestingCluster().getMaster().getCatalog().getTableDesc(tableName);
      assertNotNull(tableDesc);

      List<Column> columns = tableDesc.getSchema().getColumns();

      while(rs.next()) {
        assertEquals(tableName, rs.getString("TABLE_NAME"));
        System.out.println(">>>>" + rs.getString("COLUMN_NAME"));
        assertEquals(columns.get(numColumns).getColumnName(), rs.getString("COLUMN_NAME"));
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
}