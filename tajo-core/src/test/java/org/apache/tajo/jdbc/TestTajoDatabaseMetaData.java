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

import com.google.common.collect.Sets;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.common.type.TajoTypeUtil;
import org.apache.tajo.util.TUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

public class TestTajoDatabaseMetaData extends QueryTestCaseBase {
  private static InetSocketAddress tajoMasterAddress;

  @BeforeClass
  public static void setUp() throws Exception {
    tajoMasterAddress = testingCluster.getMaster().getTajoMasterClientService().getBindAddress();
    Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance();
  }

  public static List<String> getListFromResultSet(ResultSet resultSet, String columnName) throws SQLException {
    List<String> list = new ArrayList<String>();
    while(resultSet.next()) {
      list.add(resultSet.getString(columnName));
    }
    return list;
  }

  @Test
  public void testSetAndGetCatalogAndSchema() throws Exception {
    String connUri = TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);

    assertDatabaseNotExists("jdbc_test1");
    PreparedStatement pstmt = conn.prepareStatement("CREATE DATABASE jdbc_test1");
    pstmt.executeUpdate();
    assertDatabaseExists("jdbc_test1");
    pstmt.close();

    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertDatabaseNotExists("Jdbc_Test2");
      pstmt = conn.prepareStatement("CREATE DATABASE \"Jdbc_Test2\"");
      pstmt.executeUpdate();
      assertDatabaseExists("Jdbc_Test2");
      pstmt.close();
    }

    conn.setCatalog("jdbc_test1");
    assertEquals("jdbc_test1", conn.getCatalog());
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      conn.setCatalog("Jdbc_Test2");
      assertEquals("Jdbc_Test2", conn.getCatalog());
    }
    conn.setCatalog("jdbc_test1");
    assertEquals("jdbc_test1", conn.getCatalog());

    ResultSet resultSet = conn.getMetaData().getSchemas();
    assertResultSet(resultSet, "getSchemas1.result");
    resultSet.close();

    resultSet = conn.getMetaData().getSchemas("jdbc_test1", "%");
    assertResultSet(resultSet, "getSchemas2.result");
    resultSet.close();

    resultSet = conn.getMetaData().getTableTypes();
    assertResultSet(resultSet, "getTableTypes.result");
    resultSet.close();

    conn.setCatalog(TajoConstants.DEFAULT_DATABASE_NAME);
    pstmt = conn.prepareStatement("DROP DATABASE jdbc_test1");
    pstmt.executeUpdate();
    pstmt.close();
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      pstmt = conn.prepareStatement("DROP DATABASE \"Jdbc_Test2\"");
      pstmt.executeUpdate();
      pstmt.close();
    }

    conn.close();
  }

  @Test
  public void testGetCatalogsAndTables() throws Exception {
    String connUri = TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection defaultConnect = DriverManager.getConnection(connUri);

    DatabaseMetaData dbmd = defaultConnect.getMetaData();
    List<String> existingDatabases = getListFromResultSet(dbmd.getCatalogs(), "TABLE_CAT");

    // create database "jdbc_test1" and its tables
    assertDatabaseNotExists("jdbc_test3");
    PreparedStatement pstmt = defaultConnect.prepareStatement("CREATE DATABASE jdbc_test3");
    pstmt.executeUpdate();
    assertDatabaseExists("jdbc_test3");
    pstmt.close();
    pstmt = defaultConnect.prepareStatement("CREATE TABLE jdbc_test3.table1 (age int)");
    pstmt.executeUpdate();
    pstmt.close();
    pstmt = defaultConnect.prepareStatement("CREATE TABLE jdbc_test3.table2 (age int)");
    pstmt.executeUpdate();
    pstmt.close();

    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // create database "jdbc_test2" and its tables
      assertDatabaseNotExists("Jdbc_Test4");
      pstmt = defaultConnect.prepareStatement("CREATE DATABASE \"Jdbc_Test4\"");
      pstmt.executeUpdate();
      assertDatabaseExists("Jdbc_Test4");
      pstmt.close();

      pstmt = defaultConnect.prepareStatement("CREATE TABLE \"Jdbc_Test4\".table3 (age int)");
      pstmt.executeUpdate();
      pstmt.close();
      pstmt = defaultConnect.prepareStatement("CREATE TABLE \"Jdbc_Test4\".table4 (age int)");
      pstmt.executeUpdate();
      pstmt.close();
    }

    // verify getCatalogs()
    dbmd = defaultConnect.getMetaData();
    List<String> newDatabases = getListFromResultSet(dbmd.getCatalogs(), "TABLE_CAT");

    newDatabases.removeAll(existingDatabases);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(2, newDatabases.size());
    } else {
      assertEquals(1, newDatabases.size());
    }
    assertTrue(newDatabases.contains("jdbc_test3"));
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertTrue(newDatabases.contains("Jdbc_Test4"));
    }

    // verify getTables()
    ResultSet res = defaultConnect.getMetaData().getTables("jdbc_test3", null, null, null);
    assertResultSet(res, "getTables1.result");
    res.close();

    if (!testingCluster.isHiveCatalogStoreRunning()) {
      res = defaultConnect.getMetaData().getTables("Jdbc_Test4", null, null, null);
      assertResultSet(res, "getTables2.result");
      res.close();
    }

    defaultConnect.close();

    // jdbc1_test database connection test
    String jdbcTest1ConnUri =
        TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(), "jdbc_test3");
    Connection jdbcTest1Conn = DriverManager.getConnection(jdbcTest1ConnUri);
    assertEquals("jdbc_test3", jdbcTest1Conn.getCatalog());
    jdbcTest1Conn.close();

    client.selectDatabase("default");
    executeString("DROP TABLE jdbc_test3.table1");
    executeString("DROP TABLE jdbc_test3.table2");
    executeString("DROP DATABASE jdbc_test3");

    if (!testingCluster.isHiveCatalogStoreRunning()) {
      String jdbcTest2ConnUri =
          TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(), "Jdbc_Test4");
      Connection jdbcTest2Conn = DriverManager.getConnection(jdbcTest2ConnUri);
      assertEquals("Jdbc_Test4", jdbcTest2Conn.getCatalog());
      jdbcTest2Conn.close();

      client.selectDatabase("default");
      executeString("DROP TABLE \"Jdbc_Test4\".table3");
      executeString("DROP TABLE \"Jdbc_Test4\".table4");
      executeString("DROP DATABASE \"Jdbc_Test4\"");
    }
  }

  @Test
  public void testGetTablesWithPattern() throws Exception {
    String connUri = TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);

    Map<String,List<String>> tables = new HashMap<String,List<String>>();
    assertDatabaseNotExists("db_1");
    executeString("CREATE DATABASE db_1");
    assertDatabaseExists("db_1");
    for (int i = 0; i < 3; i++) {
      String tableName = "tb_" + i;
      TUtil.putToNestedList(tables, "db_1", tableName);
      executeString("CREATE TABLE db_1." + tableName + " (age int)");
    }
    for (int i = 0; i < 3; i++) {
      String tableName = "table_" + i + "_ptn";
      TUtil.putToNestedList(tables, "db_1", tableName);
      executeString("CREATE TABLE db_1." + tableName + " (age int)");
    }

    assertDatabaseNotExists("db_2");
    executeString("CREATE DATABASE db_2");
    assertDatabaseExists("db_2");
    for (int i = 0; i < 3; i++) {
      String tableName = "tb_" + i;
      TUtil.putToNestedList(tables, "db_2", tableName);
      executeString("CREATE TABLE db_2." + tableName + " (age int)");
    }
    for (int i = 0; i < 3; i++) {
      String tableName = "table_" + i + "_ptn";
      TUtil.putToNestedList(tables, "db_2", tableName);
      executeString("CREATE TABLE db_2." + tableName + " (age int)");
    }

    // all wildcard test
    Set<String> tableList =
        Sets.newHashSet(getListFromResultSet(conn.getMetaData().getTables("db_2", null, "%", null), "TABLE_NAME"));
    assertEquals(Sets.newHashSet(tables.get("db_2")), tableList);

    // leading wildcard test
    tableList =
        Sets.newHashSet(getListFromResultSet(conn.getMetaData().getTables("db_2", null, "%_ptn", null), "TABLE_NAME"));
    assertEquals(Sets.newHashSet("table_0_ptn", "table_1_ptn", "table_2_ptn"), tableList);

    // tailing wildcard test
    tableList =
        Sets.newHashSet(getListFromResultSet(conn.getMetaData().getTables("db_2", null, "tb_%", null), "TABLE_NAME"));
    assertEquals(Sets.newHashSet("tb_0", "tb_1", "tb_2"), tableList);

    ResultSet resultSet = conn.getMetaData().getTables(null, null, "tb\\_%", null);
    int i = 0;
    while(resultSet.next()) {
      tables.get(resultSet.getString("TABLE_CAT")).contains(resultSet.getString("TABLE_NAME"));
      i++;
    }
    assertEquals(6, i);

    executeString("DROP DATABASE db_1");
    executeString("DROP DATABASE db_2");
  }

  private static String getTestColName(String dbName, String tableName, int i) {
    if (i % 2 == 1) {
      return CatalogUtil.denormalizeIdentifier(dbName + "_" + tableName + "_col") + " int";
    } else {
      return CatalogUtil.denormalizeIdentifier(dbName + "_" + tableName + "_COL") + " int";
    }
  }

  @Test
  public void testGetColumnsWithPattern() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      String connUri = TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
          TajoConstants.DEFAULT_DATABASE_NAME);
      Connection conn = DriverManager.getConnection(connUri);

      // Below creates the following 12 tables
      // db<i>.tb<j>, i = {1,2}, 0 <= j < 2
      // db<i>.table_<j>, i = {1,2}, 0 <= j < 2

      Map<String,List<String>> tables = new HashMap<String,List<String>>();
      for (int j = 1; j <= 2; j++) {
        String dbName = "db" + j;
        assertDatabaseNotExists(dbName);
        executeString("CREATE DATABASE " + dbName).close();
        assertDatabaseExists(dbName);
        for (int i = 3; i < 6; i++) {
          String tableName = "tb" + i;


          if (i % 2 == 0) {
            tableName = tableName.toUpperCase();
          }

          TUtil.putToNestedList(tables, dbName, tableName);

          executeString("CREATE TABLE " + dbName + "." + CatalogUtil.denormalizeIdentifier(tableName) +
              " (" + getTestColName(dbName, tableName, 1) +
              ") PARTITION BY COLUMN (" + getTestColName(dbName, tableName, 2) + ")").close();
          assertTableExists(dbName + "." + tableName);
        }
        for (int i = 3; i < 6; i++) {
          String tableName = "table" + i;


          if (i % 2 == 0) {
            tableName = tableName.toUpperCase();
          }

          TUtil.putToNestedList(tables, dbName, tableName);

          executeString("CREATE TABLE " + dbName + "." + CatalogUtil.denormalizeIdentifier(tableName) +
              " (" + getTestColName(dbName, tableName, 1) +
              ") PARTITION BY COLUMN (" + getTestColName(dbName, tableName, 2) + ")").close();
          assertTableExists(dbName + "." + tableName);
        }
      }

      // all wildcard test on columns
      Set<String> columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db2", null, "tb3", "%"),
              "COLUMN_NAME"));
      assertEquals(Sets.newHashSet("db2_tb3_col", "db2_tb3_COL"), columnList);

      // tailing wildcard + case sensitive test on columns
      columnList = Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db2", null, "tb3", "%col"),
          "COLUMN_NAME"));
      assertEquals(Sets.newHashSet("db2_tb3_col"), columnList);
      columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db2", null, "tb3", "%COL"),
              "COLUMN_NAME"));
      assertEquals(Sets.newHashSet("db2_tb3_COL"), columnList);

      // tailing wildcard test on columns
      columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db2", null, "tb3", "db2\\_tb3\\_%"),
              "COLUMN_NAME"));
      assertEquals(Sets.newHashSet("db2_tb3_col", "db2_tb3_COL"), columnList);
      columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db2", null, "%", "db2\\_tb3\\_%"),
              "COLUMN_NAME"));
      assertEquals(Sets.newHashSet("db2_tb3_col", "db2_tb3_COL"), columnList);

      // leading wildcard test on tables
      columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db1", null, "%3", "%"),
              "COLUMN_NAME"));
      assertEquals(
          Sets.newHashSet(
              "db1_tb3_col", "db1_tb3_COL",
              "db1_table3_col", "db1_table3_COL"),
          columnList);
      columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db2", null, "%3", "%"),
              "COLUMN_NAME"));
      assertEquals(
          Sets.newHashSet(
              "db2_tb3_col", "db2_tb3_COL",
              "db2_table3_col", "db2_table3_COL"),
          columnList);

      // tailing wildcard + case sensitive test on tables
      columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db2", null, "TABLE%", "%"),
              "COLUMN_NAME"));
      assertEquals(
          Sets.newHashSet(
              "db2_TABLE4_col", "db2_TABLE4_COL"), columnList);

      columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db2", null, "TABLE4", "%"),
              "COLUMN_NAME"));
      assertEquals(
          Sets.newHashSet(
              "db2_TABLE4_col", "db2_TABLE4_COL"),
          columnList);

      // tailing wildcard test on tables
      columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns("db2", null, "table%", "%"),
              "COLUMN_NAME"));
      assertEquals(
          Sets.newHashSet(
              "db2_table3_col", "db2_table3_COL",
              "db2_table5_col", "db2_table5_COL"),
          columnList);

      // wildcard test on database
      columnList =
          Sets.newHashSet(getListFromResultSet(conn.getMetaData().getColumns(null, null, "%3", "db1_tb3%"),
              "COLUMN_NAME"));
      assertEquals(Sets.newHashSet("db1_tb3_col", "db1_tb3_COL"), columnList);

      executeString("DROP DATABASE db1");
      executeString("DROP DATABASE db2");
    }
  }

  @Test
  public void testEmptyMetaInfo() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      String connUri = TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
          TajoConstants.DEFAULT_DATABASE_NAME);
      Connection conn = DriverManager.getConnection(connUri);

      try {
        DatabaseMetaData meta = conn.getMetaData();

        ResultSet res = meta.getProcedures(null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getProcedureColumns(null, null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getUDTs(null, null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getColumnPrivileges(null, null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getTablePrivileges(null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getBestRowIdentifier(null, null, null, 0, false);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getVersionColumns(null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getPrimaryKeys(null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getImportedKeys(null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getExportedKeys(null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getCrossReference(null, null, null, null, null, null);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getIndexInfo(null, null, null, false, false);
        assertNotNull(res);
        assertFalse(res.next());

        res = meta.getClientInfoProperties();
        assertNotNull(res);
        assertFalse(res.next());
      } finally {
        conn.close();
      }
    }
  }

  @Test
  public void testGetTypeInfo() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      String connUri = TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
          TajoConstants.DEFAULT_DATABASE_NAME);
      Connection conn = DriverManager.getConnection(connUri);

      try {
        DatabaseMetaData meta = conn.getMetaData();

        ResultSet res = meta.getTypeInfo();

        assertNotNull(res);

        int numTypes = 0;

        String[] columnNames = {"TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
            "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
            "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
            "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"};

        while (res.next()) {
          for (int i = 0; i < columnNames.length; i++) {
            Object value = res.getObject(columnNames[i]);
            if (i == 15 || i == 16) {
              assertNull(value);
            } else {
              assertNotNull(value);
            }
          }
          numTypes++;
        }

        assertEquals(numTypes, TajoTypeUtil.getTypeInfos().size());
      } finally {
        conn.close();
      }
    }
  }
}
