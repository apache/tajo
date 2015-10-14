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

package org.apache.tajo.engine.query;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.exception.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestAlterTable extends QueryTestCaseBase {

  @Before
  public void setUp() throws Exception {
    executeString("create database " + getCurrentDatabase()).close();
  }

  @After
  public void tearDown() throws Exception {
    executeString("drop database " + getCurrentDatabase()).close();
  }

  @Test
  public final void testAlterTableName() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "ABC");
    assertTableExists(createdNames.get(0));
    executeDDL("alter_table_rename_table_ddl.sql", null);
    assertTableExists("DEF");
  }

  @Test
  public final void testAlterTableColumnName() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "XYZ");
    executeDDL("alter_table_rename_column_ddl.sql", null);
    assertColumnExists(createdNames.get(0), "renum");
  }

  @Test
  public final void testAlterTableAddNewColumn() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "EFG");
    executeDDL("alter_table_add_new_column_ddl.sql", null);
    assertColumnExists(createdNames.get(0), "cool");
  }

  @Test
  public final void testAlterTableSetProperty() throws Exception {
    executeDDL("table2_ddl.sql", "table2.tbl", "ALTX");
    ResultSet before_res = executeQuery();
    assertResultSet(before_res, "before_set_property_delimiter.result");
    cleanupQuery(before_res);

    executeDDL("alter_table_set_property_delimiter.sql", null);

    ResultSet after_res = executeQuery();
    assertResultSet(after_res, "after_set_property_delimiter.result");
    cleanupQuery(after_res);
  }

  // TODO: This should be added at TAJO-1891
  public final void testAlterTableAddPartition() throws Exception {
    executeDDL("create_partitioned_table.sql", null);

    String tableName = CatalogUtil.buildFQName("TestAlterTable", "partitioned_table");
    assertTrue(catalog.existsTable(tableName));

    TableDesc retrieved = catalog.getTableDesc(tableName);
    assertEquals(retrieved.getName(), tableName);
    assertEquals(retrieved.getPartitionMethod().getPartitionType(), CatalogProtos.PartitionType.COLUMN);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getAllColumns().size(), 2);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName(), "col3");
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(1).getSimpleName(), "col4");

    executeDDL("alter_table_add_partition1.sql", null);
    executeDDL("alter_table_add_partition2.sql", null);

    List<CatalogProtos.PartitionDescProto> partitions = catalog.getPartitions("TestAlterTable", "partitioned_table");
    assertNotNull(partitions);
    assertEquals(partitions.size(), 1);
    assertEquals(partitions.get(0).getPartitionName(), "col3=1/col4=2");
    assertEquals(partitions.get(0).getPartitionKeysList().get(0).getColumnName(), "col3");
    assertEquals(partitions.get(0).getPartitionKeysList().get(0).getPartitionValue(), "1");
    assertEquals(partitions.get(0).getPartitionKeysList().get(1).getColumnName(), "col4");
    assertEquals(partitions.get(0).getPartitionKeysList().get(1).getPartitionValue(), "2");

    assertNotNull(partitions.get(0).getPath());
    Path partitionPath = new Path(partitions.get(0).getPath());
    FileSystem fs = partitionPath.getFileSystem(conf);
    assertTrue(fs.exists(partitionPath));
    assertTrue(partitionPath.toString().indexOf("col3=1/col4=2") > 0);

    executeDDL("alter_table_drop_partition1.sql", null);
    executeDDL("alter_table_drop_partition2.sql", null);

    partitions = catalog.getPartitions("TestAlterTable", "partitioned_table");
    assertNotNull(partitions);
    assertEquals(partitions.size(), 0);
    assertFalse(fs.exists(partitionPath));

    catalog.dropTable(tableName);
  }

  @Test
  public final void testAlterTableRepairPartition() throws Exception {
    executeDDL("create_partitioned_table2.sql", null);

    String simpleTableName = "partitioned_table2";
    String tableName = CatalogUtil.buildFQName(getCurrentDatabase(), simpleTableName);
    assertTrue(catalog.existsTable(tableName));

    TableDesc tableDesc = catalog.getTableDesc(tableName);
    assertEquals(tableDesc.getName(), tableName);
    assertEquals(tableDesc.getPartitionMethod().getPartitionType(), CatalogProtos.PartitionType.COLUMN);
    assertEquals(tableDesc.getPartitionMethod().getExpressionSchema().getAllColumns().size(), 2);
    assertEquals(tableDesc.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName(), "col1");
    assertEquals(tableDesc.getPartitionMethod().getExpressionSchema().getColumn(1).getSimpleName(), "col2");

    ResultSet res = executeString(
      "insert overwrite into " + simpleTableName + " select l_quantity, l_returnflag, l_orderkey, l_partkey " +
      " from default.lineitem");
    res.close();

    res = executeString("select * from " + simpleTableName + " order by col1, col2, col3, col4");
    String result = resultSetToString(res);
    String expectedResult = "col3,col4,col1,col2\n" +
      "-------------------------------\n" +
      "17.0,N,1,1\n" +
      "36.0,N,1,1\n" +
      "38.0,N,2,2\n" +
      "45.0,R,3,2\n" +
      "49.0,R,3,3\n";

    res.close();
    assertEquals(expectedResult, result);

    verifyPartitionCount(getCurrentDatabase(), simpleTableName, 4);

    Path tablePath = new Path(tableDesc.getUri());
    FileSystem fs = tablePath.getFileSystem(conf);
    assertTrue(fs.exists(new Path(tableDesc.getUri())));
    assertTrue(fs.isDirectory(new Path(tablePath.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(tablePath.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(tablePath.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(tablePath.toUri() + "/col1=3/col2=3")));

    // Remove all partitions
    executeString("ALTER TABLE " + simpleTableName + " DROP PARTITION (col1 = 1 , col2 = 1)").close();
    executeString("ALTER TABLE " + simpleTableName + " DROP PARTITION (col1 = 2 , col2 = 2)").close();
    executeString("ALTER TABLE " + simpleTableName + " DROP PARTITION (col1 = 3 , col2 = 2)").close();
    executeString("ALTER TABLE " + simpleTableName + " DROP PARTITION (col1 = 3 , col2 = 3)").close();

    verifyPartitionCount(getCurrentDatabase(), simpleTableName, 0);

    assertTrue(fs.exists(new Path(tableDesc.getUri())));
    assertTrue(fs.isDirectory(new Path(tablePath.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(tablePath.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(tablePath.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(tablePath.toUri() + "/col1=3/col2=3")));

    executeString("ALTER TABLE " + simpleTableName + " REPAIR PARTITION").close();
    verifyPartitionCount(getCurrentDatabase(), simpleTableName, 4);

    // Remove just one of existing partitions
    executeString("ALTER TABLE " + simpleTableName + " DROP PARTITION (col1 = 3 , col2 = 3)").close();
    executeString("ALTER TABLE " + simpleTableName + " REPAIR PARTITION").close();
    verifyPartitionCount(getCurrentDatabase(), simpleTableName, 4);

    // Remove a partition directory from filesystem
    fs.delete(new Path(tablePath.toUri() + "/col1=3/col2=3"), true);
    executeString("ALTER TABLE " + simpleTableName + " REPAIR PARTITION").close();
    verifyPartitionCount(getCurrentDatabase(), simpleTableName, 4);

    // Add abnormal directories
    assertTrue(fs.mkdirs(new Path(tablePath.toUri() + "/col10=1/col20=1")));
    assertTrue(fs.mkdirs(new Path(tablePath.toUri() + "/col1=")));
    assertTrue(fs.mkdirs(new Path(tablePath.toUri() + "/test")));
    assertEquals(6, fs.listStatus(new Path(tablePath.toUri())).length);

    executeString("ALTER TABLE " + simpleTableName + " REPAIR PARTITION").close();
    verifyPartitionCount(getCurrentDatabase(), simpleTableName, 4);
    catalog.dropTable(tableName);
  }

  @Test
  public final void testRepairPartitionWithDatabaseNameIncludeTableName() throws Exception {
    String databaseName = "test_repair_partition";
    String tableName = "part";
    String canonicalTableName = CatalogUtil.getCanonicalTableName(databaseName, tableName);

    executeString("create database " + databaseName).close();
    executeString("create table " + canonicalTableName + "(col1 int4, col2 int4) partition by column(key float8) "
      + " as select l_orderkey, l_partkey, l_quantity from default.lineitem").close();

    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    assertNotNull(tableDesc);

    verifyPartitionCount(databaseName, tableName, 5);

    ResultSet res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    String result = resultSetToString(res);
    String expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,36.0\n" +
      "1,1,17.0\n" +
      "2,2,38.0\n" +
      "3,3,49.0\n" +
      "3,2,45.0\n";
    res.close();
    assertEquals(expectedResult, result);

    // Remove all partitions
    dropPartitions(databaseName, tableName, tableDesc.getPartitionMethod().getExpressionSchema().getAllColumns());

    verifyPartitionCount(databaseName, tableName, 0);

    executeString("ALTER TABLE " + canonicalTableName + " REPAIR PARTITION").close();

    verifyPartitionCount(databaseName, tableName, 5);

    res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    result = resultSetToString(res);
    res.close();
    assertEquals(expectedResult, result);

    executeString("DROP TABLE " + canonicalTableName + " PURGE").close();
    executeString("DROP database " + databaseName).close();
  }

  @Test
  public void testRepairPartitionWithAbnormalDirectories()  throws Exception {
    String databaseName = getCurrentDatabase().toLowerCase();
    String tableName = "testRepairPartitionWithAbnormalDirectories".toLowerCase();
    String canonicalTableName = CatalogUtil.getCanonicalTableName(databaseName, tableName);

    executeString("create table " + canonicalTableName + "(col1 int4, col2 int4) partition by column(key float8) "
      + " as select l_orderkey, l_partkey, l_quantity from default.lineitem").close();

    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    assertNotNull(tableDesc);

    verifyPartitionCount(databaseName, tableName, 5);

    ResultSet res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    String result = resultSetToString(res);
    String expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,36.0\n" +
      "1,1,17.0\n" +
      "2,2,38.0\n" +
      "3,3,49.0\n" +
      "3,2,45.0\n";
    res.close();
    assertEquals(expectedResult, result);

    // Remove all partitions
    dropPartitions(databaseName, tableName, tableDesc.getPartitionMethod().getExpressionSchema().getAllColumns());

    verifyPartitionCount(databaseName, tableName, 0);

    // Make abnormal directories
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(tableDesc.getUri().getPath(), "key=100.0");
    fs.mkdirs(path);
    path = new Path(tableDesc.getUri().getPath(), "key=110.0");
    fs.mkdirs(path);
    path = new Path(tableDesc.getUri().getPath(), "key=");
    fs.mkdirs(path);
    path = new Path(tableDesc.getUri().getPath(), "col1=a");
    fs.mkdirs(path);
    assertEquals(9, fs.listStatus(path.getParent()).length);

    executeString("ALTER TABLE " + canonicalTableName + " REPAIR PARTITION").close();

    verifyPartitionCount(databaseName, tableName, 7);

    res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    result = resultSetToString(res);
    res.close();
    assertEquals(expectedResult, result);

    executeString("DROP TABLE " + canonicalTableName + " PURGE").close();
  }

  @Test
  public void testRepairPartitionWithDatePartitionColumn()  throws Exception {
    String databaseName = getCurrentDatabase().toLowerCase();
    String tableName = "testRepairPartitionWithDatePartitionColumn".toLowerCase();
    String canonicalTableName = CatalogUtil.getCanonicalTableName(databaseName, tableName);

    executeString(
      "create table " + canonicalTableName + "(col1 int4, col2 int4) partition by column(key date) "
        + " as select l_orderkey, l_partkey, l_shipdate::date from default.lineitem").close();

    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    assertNotNull(tableDesc);

    verifyPartitionCount(databaseName, tableName, 5);

    ResultSet res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    String result = resultSetToString(res);
    String expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,1996-04-12\n" +
      "1,1,1996-03-13\n" +
      "2,2,1997-01-28\n" +
      "3,3,1993-11-09\n" +
      "3,2,1994-02-02\n";
    res.close();
    assertEquals(expectedResult, result);

    // Remove all partitions
    dropPartitions(databaseName, tableName, tableDesc.getPartitionMethod().getExpressionSchema().getAllColumns());

    verifyPartitionCount(databaseName, tableName, 0);

    executeString("ALTER TABLE " + canonicalTableName + " REPAIR PARTITION").close();

    verifyPartitionCount(databaseName, tableName, 5);

    res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    result = resultSetToString(res);
    res.close();
    assertEquals(expectedResult, result);

    executeString("DROP TABLE " + canonicalTableName + " PURGE").close();
  }

  @Test
  public void testRepairPartitionWithTimestampPartitionColumn()  throws Exception {
    String databaseName = getCurrentDatabase().toLowerCase();
    String tableName = "testRepairPartitionWithTimestampPartitionColumn".toLowerCase();
    String canonicalTableName = CatalogUtil.getCanonicalTableName(databaseName, tableName);

    executeString(
      "create table " + canonicalTableName + "(col1 int4, col2 int4) partition by column(key timestamp) "
        + " as select l_orderkey, l_partkey, to_timestamp(l_shipdate, 'YYYY-MM-DD') from default.lineitem");

    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    assertNotNull(tableDesc);

    verifyPartitionCount(databaseName, tableName, 5);

    ResultSet res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    String result = resultSetToString(res);
    String expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,1996-04-12 00:00:00\n" +
      "1,1,1996-03-13 00:00:00\n" +
      "2,2,1997-01-28 00:00:00\n" +
      "3,3,1993-11-09 00:00:00\n" +
      "3,2,1994-02-02 00:00:00\n";
    res.close();
    assertEquals(expectedResult, result);

    // Remove all partitions
    dropPartitions(databaseName, tableName, tableDesc.getPartitionMethod().getExpressionSchema().getAllColumns());

    verifyPartitionCount(databaseName, tableName, 0);

    executeString("ALTER TABLE " + canonicalTableName + " REPAIR PARTITION").close();

    verifyPartitionCount(databaseName, tableName, 5);

    res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    result = resultSetToString(res);
    res.close();
    assertEquals(expectedResult, result);

    executeString("DROP TABLE " + canonicalTableName + " PURGE").close();
  }

  @Test
  public void testRepairPartitionWithTimesPartitionColumn()  throws Exception {
    String databaseName = getCurrentDatabase().toLowerCase();
    String tableName = "testRepairPartitionWithTimesPartitionColumn".toLowerCase();
    String canonicalTableName = CatalogUtil.getCanonicalTableName(databaseName, tableName);

    executeString(
      "create table " + canonicalTableName + "(col1 int4, col2 int4) partition by column(key time) "
        + " as select l_orderkey, l_partkey " +
        " , CASE l_shipdate WHEN '1996-03-13' THEN cast ('11:20:40' as time) " +
        " WHEN '1997-01-28' THEN cast ('12:10:20' as time) " +
        " WHEN '1994-02-02' THEN cast ('12:10:30' as time) " +
        " ELSE cast ('00:00:00' as time) END " +
        " from default.lineitem");

    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    assertNotNull(tableDesc);

    ResultSet res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    String result = resultSetToString(res);
    String expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,11:20:40\n" +
      "1,1,00:00:00\n" +
      "2,2,12:10:20\n" +
      "3,3,00:00:00\n" +
      "3,2,12:10:30\n";
    res.close();
    assertEquals(expectedResult, result);

    verifyPartitionCount(databaseName, tableName, 4);

    // Remove all partitions
    dropPartitions(databaseName, tableName, tableDesc.getPartitionMethod().getExpressionSchema().getAllColumns());

    verifyPartitionCount(databaseName, tableName, 0);

    executeString("ALTER TABLE " + canonicalTableName + " REPAIR PARTITION").close();

    verifyPartitionCount(databaseName, tableName, 4);

    res = executeString("SELECT * FROM " + canonicalTableName + " ORDER BY col1, col2 desc, key desc;");
    result = resultSetToString(res);
    res.close();
    assertEquals(expectedResult, result);

    executeString("DROP TABLE " + canonicalTableName + " PURGE").close();
  }


  @Test
  public void testRepairPartitionWithMutiplePartitionColumn()  throws Exception {
    String databaseName = getCurrentDatabase().toLowerCase();
    String tableName = "testRepairPartitionWithMutiplePartitionColumn".toLowerCase();
    String canonicalTableName = CatalogUtil.getCanonicalTableName(databaseName, tableName);

    executeString("create table " + canonicalTableName + " (col4 text) "
      + " partition by column(col1 int4, col2 int4, col3 float8) as select l_returnflag, l_orderkey, l_partkey, " +
      "l_quantity from default.lineitem");

    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    assertNotNull(tableDesc);

    ResultSet res = executeString("SELECT * FROM " + canonicalTableName
      + " ORDER BY col1, col2 desc, col3 desc, col4;");
    String result = resultSetToString(res);
    String expectedResult = "col4,col1,col2,col3\n" +
      "-------------------------------\n" +
      "N,1,1,36.0\n" +
      "N,1,1,17.0\n" +
      "N,2,2,38.0\n" +
      "R,3,3,49.0\n" +
      "R,3,2,45.0\n";
    res.close();
    assertEquals(expectedResult, result);

    verifyPartitionCount(databaseName, tableName, 5);

    // Remove all partitions
    dropPartitions(databaseName, tableName, tableDesc.getPartitionMethod().getExpressionSchema().getAllColumns());

    verifyPartitionCount(databaseName, tableName, 0);

    executeString("ALTER TABLE " + canonicalTableName + " REPAIR PARTITION").close();

    verifyPartitionCount(databaseName, tableName, 5);

    res = executeString("SELECT * FROM " + canonicalTableName
      + " ORDER BY col1, col2 desc, col3 desc, col4;");    result = resultSetToString(res);
    res.close();
    assertEquals(expectedResult, result);

    executeString("DROP TABLE " + canonicalTableName + " PURGE").close();
  }


  private void verifyPartitionCount(String databaseName, String tableName, int expectedCount)
    throws UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException,
    UndefinedPartitionException {
    List<CatalogProtos.PartitionDescProto> partitions = catalog.getPartitions(databaseName, tableName);
    assertNotNull(partitions);
    assertEquals(partitions.size(), expectedCount);
  }

  private void dropPartitions(String databaseName, String tableName, List<Column> colums)
    throws Exception {
    String canonicalTableName = CatalogUtil.getCanonicalTableName(databaseName, tableName);
    List<CatalogProtos.PartitionDescProto> partitions = catalog.getPartitions(databaseName, tableName);

    StringBuilder sb = new StringBuilder();
    for (CatalogProtos.PartitionDescProto partition : partitions) {

      sb.delete(0, sb.length());
      sb.append("ALTER TABLE ").append(canonicalTableName).append(" DROP PARTITION (");

      String[] splitPartitionName = partition.getPartitionName().split(File.separator);
      for(int i = 0; i < splitPartitionName.length; i++) {
        String[] partitionColumnValue = splitPartitionName[i].split("=");
        if (i > 0) {
          sb.append(",");
        }

        switch (colums.get(i).getDataType().getType()) {
          case TEXT:
          case TIME:
          case TIMESTAMP:
          case DATE:
            sb.append(partitionColumnValue[0]).append("='").append(partitionColumnValue[1]).append("'");
            break;
          default:
            sb.append(partitionColumnValue[0]).append("=").append(partitionColumnValue[1]);
            break;
        }
      }
      sb.append(")");
      executeString(sb.toString()).close();
    }
  }

}
