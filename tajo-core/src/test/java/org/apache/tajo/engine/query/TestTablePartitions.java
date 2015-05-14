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

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.jdbc.FetchResultSet;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.querymaster.QueryMasterTask;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.worker.TajoWorker;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.plan.serder.PlanProto.ShuffleType.SCATTERED_HASH_SHUFFLE;
import static org.junit.Assert.*;

public class TestTablePartitions extends QueryTestCaseBase {


  public TestTablePartitions() throws IOException {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testCreateColumnPartitionedTable() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testCreateColumnPartitionedTable");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
    assertEquals(2, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
    assertEquals(3, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

    res = testBase.execute(
        "insert overwrite into " + tableName + " select l_orderkey, l_partkey, " +
            "l_quantity from lineitem");

    MasterPlan plan = getQueryPlan(res);
    ExecutionBlock rootEB = plan.getRoot();

    /*
    -------------------------------------------------------------------------------
    |-eb_1405354886454_0001_000003
       |-eb_1405354886454_0001_000002
          |-eb_1405354886454_0001_000001
     */
    assertEquals(1, plan.getChildCount(rootEB.getId()));

    ExecutionBlock insertEB = plan.getChild(rootEB.getId(), 0);
    assertNotNull(insertEB);
    assertEquals(NodeType.INSERT, insertEB.getPlan().getType());
    assertEquals(1, plan.getChildCount(insertEB.getId()));

    ExecutionBlock scanEB = plan.getChild(insertEB.getId(), 0);

    List<DataChannel> list = plan.getOutgoingChannels(scanEB.getId());
    assertEquals(1, list.size());
    DataChannel channel = list.get(0);
    assertNotNull(channel);
    assertEquals(SCATTERED_HASH_SHUFFLE, channel.getShuffleType());
    assertEquals(1, channel.getShuffleKeys().length);

    res.close();
  }

  @Test
  public final void testCreateColumnPartitionedTableWithJoin() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testCreateColumnPartitionedTableWithJoin");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
    assertEquals(2, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
    assertEquals(3, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

    res = testBase.execute(
        "insert overwrite into " + tableName + " select l_orderkey, l_partkey, " +
            "l_quantity from lineitem join orders on l_orderkey = o_orderkey");

    MasterPlan plan = getQueryPlan(res);
    ExecutionBlock rootEB = plan.getRoot();

    /*
    -------------------------------------------------------------------------------
    |-eb_1405356074433_0001_000005
       |-eb_1405356074433_0001_000004
          |-eb_1405356074433_0001_000003
             |-eb_1405356074433_0001_000002
             |-eb_1405356074433_0001_000001
     */
    assertEquals(1, plan.getChildCount(rootEB.getId()));

    ExecutionBlock insertEB = plan.getChild(rootEB.getId(), 0);
    assertNotNull(insertEB);
    assertEquals(NodeType.INSERT, insertEB.getPlan().getType());
    assertEquals(1, plan.getChildCount(insertEB.getId()));

    ExecutionBlock scanEB = plan.getChild(insertEB.getId(), 0);

    List<DataChannel> list = plan.getOutgoingChannels(scanEB.getId());
    assertEquals(1, list.size());
    DataChannel channel = list.get(0);
    assertNotNull(channel);
    assertEquals(SCATTERED_HASH_SHUFFLE, channel.getShuffleType());
    assertEquals(1, channel.getShuffleKeys().length);

    res.close();
  }

  @Test
  public final void testCreateColumnPartitionedTableWithSelectedColumns() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testCreateColumnPartitionedTableWithSelectedColumns");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
    assertEquals(3, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
    assertEquals(4, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

    res = executeString("insert overwrite into " + tableName + " (col1, col2, key) select l_orderkey, " +
        "l_partkey, l_quantity from lineitem");
    res.close();
  }

  @Test
  public final void testColumnPartitionedTableByOneColumn() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableByOneColumn");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName
        + " (col1, col2, key) select l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    assertPartitionDirectories(desc);

    res = executeString(
        "select distinct * from " + tableName + " where (key = 45.0 or key = 38.0) and null_col is null");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});

    for (int i = 0; i < 2; i++) {
      assertTrue(res.next());
      assertEquals(resultRows1.get(res.getDouble(4))[0], res.getInt(1));
      assertEquals(resultRows1.get(res.getDouble(4))[1], res.getInt(2));
    }
    res.close();
  }

  private void assertPartitionDirectories(TableDesc desc) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(desc.getPath());
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=36.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=49.0")));
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }
  }

  @Test
  public final void testQueryCasesOnColumnPartitionedTable() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testQueryCasesOnColumnPartitionedTable");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName
            + " (col1, col2, key) select l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    assertPartitionDirectories(desc);

    res = executeFile("case1.sql");
    assertResultSet(res, "case1.result");
    res.close();

    res = executeFile("case2.sql");
    assertResultSet(res, "case2.result");
    res.close();

    res = executeFile("case3.sql");
    assertResultSet(res, "case3.result");
    res.close();

    // select pow(key, 2) from testQueryCasesOnColumnPartitionedTable
    res = executeFile("case4.sql");
    assertResultSet(res, "case4.result");
    res.close();

    // select round(pow(key + 1, 2)) from testQueryCasesOnColumnPartitionedTable
    res = executeFile("case5.sql");
    assertResultSet(res, "case5.result");
    res.close();

    // select col1, key from testQueryCasesOnColumnPartitionedTable order by pow(key, 2) desc
    res = executeFile("case6.sql");
    assertResultSet(res, "case6.result");
    res.close();

    // select col1, key from testQueryCasesOnColumnPartitionedTable WHERE key BETWEEN 35 AND 48;
    res = executeFile("case7.sql");
    assertResultSet(res, "case7.result");
    res.close();

    // select col1, CASE key WHEN 36 THEN key WHEN 49 THEN key ELSE key END from testQueryCasesOnColumnPartitionedTable;
    res = executeFile("case8.sql");
    assertResultSet(res, "case8.result");
    res.close();

    // select col1, CAST(key AS INT) from testQueryCasesOnColumnPartitionedTable;
    res = executeFile("case9.sql");
    assertResultSet(res, "case9.result");
    res.close();

    // select col1, (!(key > 35)) from testQueryCasesOnColumnPartitionedTable;
    res = executeFile("case10.sql");
    assertResultSet(res, "case10.result");
    res.close();

    // alias partition column
    res = executeFile("case11.sql");
    assertResultSet(res, "case11.result");
    res.close();

    // alias partition column in group by, order by
    res = executeFile("case12.sql");
    assertResultSet(res, "case12.result");
    res.close();

    // alias partition column in stage
    res = executeFile("case13.sql");
    assertResultSet(res, "case13.result");
    res.close();
  }

  @Test
  public final void testColumnPartitionedTableByThreeColumns() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableByThreeColumns");
    ResultSet res = testBase.execute(
        "create table " + tableName + " (col4 text) partition by column(col1 int4, col2 int4, col3 float8) ");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName
        + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    Path path = new Path(desc.getPath());

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    res = executeString("select * from " + tableName + " where col2 = 2");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});


    for (int i = 0; i < 2; i++) {
      assertTrue(res.next());
      assertEquals(resultRows1.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows1.get(res.getDouble(4))[1], res.getInt(3));
    }
    res.close();


    Map<Double, int []> resultRows2 = Maps.newHashMap();
    resultRows2.put(49.0d, new int[]{3, 3});
    resultRows2.put(45.0d, new int[]{3, 2});
    resultRows2.put(38.0d, new int[]{2, 2});

    res = executeString("select * from " + tableName + " where (col1 = 2 or col1 = 3) and col2 >= 2");

    for (int i = 0; i < 3; i++) {
      assertTrue(res.next());
      assertEquals(resultRows2.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows2.get(res.getDouble(4))[1], res.getInt(3));
    }
    res.close();
  }

  @Test
  public final void testInsertIntoColumnPartitionedTableByThreeColumns() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testInsertIntoColumnPartitionedTableByThreeColumns");
    ResultSet res = testBase.execute(
        "create table " + tableName + " (col4 text) partition by column(col1 int4, col2 int4, col3 float8) ");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert into " + tableName
        + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    Path path = new Path(desc.getPath());

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    res = executeString("select * from " + tableName + " where col2 = 2");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});

    for (int i = 0; i < 2; i++) {
      assertTrue(res.next());
      assertEquals(resultRows1.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows1.get(res.getDouble(4))[1], res.getInt(3));
    }
    res.close();


    Map<Double, int []> resultRows2 = Maps.newHashMap();
    resultRows2.put(49.0d, new int[]{3, 3});
    resultRows2.put(45.0d, new int[]{3, 2});
    resultRows2.put(38.0d, new int[]{2, 2});

    res = executeString("select * from " + tableName + " where (col1 = 2 or col1 = 3) and col2 >= 2");

    for (int i = 0; i < 3; i++) {
      assertTrue(res.next());
      assertEquals(resultRows2.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows2.get(res.getDouble(4))[1], res.getInt(3));
    }
    res.close();

    // insert into already exists partitioned table
    res = executeString("insert into " + tableName
        + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();

    desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    path = new Path(desc.getPath());

    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));

    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }
    String expected = "N\n" +
        "N\n" +
        "N\n" +
        "N\n" +
        "N\n" +
        "N\n" +
        "R\n" +
        "R\n" +
        "R\n" +
        "R\n";

    String tableData = getTableFileContents(new Path(desc.getPath()));
    assertEquals(expected, tableData);

    res = executeString("select * from " + tableName + " where col2 = 2");
    String resultSetData = resultSetToString(res);
    res.close();
    expected = "col4,col1,col2,col3\n" +
        "-------------------------------\n" +
        "N,2,2,38.0\n" +
        "N,2,2,38.0\n" +
        "R,3,2,45.0\n" +
        "R,3,2,45.0\n";
    assertEquals(expected, resultSetData);

    res = executeString("select * from " + tableName + " where (col1 = 2 or col1 = 3) and col2 >= 2");
    resultSetData = resultSetToString(res);
    res.close();
    expected = "col4,col1,col2,col3\n" +
        "-------------------------------\n" +
        "N,2,2,38.0\n" +
        "N,2,2,38.0\n" +
        "R,3,2,45.0\n" +
        "R,3,2,45.0\n" +
        "R,3,3,49.0\n" +
        "R,3,3,49.0\n";
    assertEquals(expected, resultSetData);

    // Check not to remove existing partition directories.
    res = executeString("insert overwrite into " + tableName
        + " select l_returnflag, l_orderkey, l_partkey, 30.0 as l_quantity from lineitem "
        + " where l_orderkey = 1 and l_partkey = 1 and  l_linenumber = 1");
    res.close();

    desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=30.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));

    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // TODO: If there is existing another partition directory, we must add its rows number to result row numbers.
      // assertEquals(6, desc.getStats().getNumRows().intValue());
    }

    res = executeString("select * from " + tableName + " where col2 = 1");
    resultSetData = resultSetToString(res);
    res.close();
    expected = "col4,col1,col2,col3\n" +
        "-------------------------------\n" +
        "N,1,1,17.0\n" +
        "N,1,1,17.0\n" +
        "N,1,1,30.0\n" +
        "N,1,1,36.0\n" +
        "N,1,1,36.0\n";

    assertEquals(expected, resultSetData);

    // insert overwrite empty result to partitioned table
    res = executeString("insert overwrite into " + tableName
      + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem where l_orderkey" +
      " > 100");
    res.close();

    desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);

    ContentSummary summary = fs.getContentSummary(new Path(desc.getPath()));

    assertEquals(summary.getDirectoryCount(), 1L);
    assertEquals(summary.getFileCount(), 0L);
    assertEquals(summary.getLength(), 0L);
  }

  @Test
  public final void testColumnPartitionedTableByOneColumnsWithCompression() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableByOneColumnsWithCompression");
    ResultSet res = executeString(
        "create table " + tableName + " (col2 int4, col3 float8) USING csv " +
            "WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
            "PARTITION BY column(col1 int4)");
    res.close();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName + " select l_partkey, l_quantity, l_orderkey from lineitem");
    res.close();
    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path(desc.getPath())));
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    Path path = new Path(desc.getPath());
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));

    for (FileStatus partition : fs.listStatus(path)){
      assertTrue(fs.isDirectory(partition.getPath()));
      for (FileStatus file : fs.listStatus(partition.getPath())) {
        CompressionCodec codec = factory.getCodec(file.getPath());
        assertTrue(codec instanceof DeflateCodec);
      }
    }
  }

  @Test
  public final void testColumnPartitionedTableByTwoColumnsWithCompression() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableByTwoColumnsWithCompression");
    ResultSet res = executeString("create table " + tableName + " (col3 float8, col4 text) USING csv " +
        "WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
        "PARTITION by column(col1 int4, col2 int4)");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName +
            " select  l_quantity, l_returnflag, l_orderkey, l_partkey from lineitem");
    res.close();
    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path(desc.getPath())));
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    Path path = new Path(desc.getPath());
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));

    for (FileStatus partition1 : fs.listStatus(path)){
      assertTrue(fs.isDirectory(partition1.getPath()));
      for (FileStatus partition2 : fs.listStatus(partition1.getPath())) {
        assertTrue(fs.isDirectory(partition2.getPath()));
        for (FileStatus file : fs.listStatus(partition2.getPath())) {
          CompressionCodec codec = factory.getCodec(file.getPath());
          assertTrue(codec instanceof DeflateCodec);
        }
      }
    }
  }

  @Test
  public final void testColumnPartitionedTableByThreeColumnsWithCompression() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableByThreeColumnsWithCompression");
    ResultSet res = executeString(
        "create table " + tableName + " (col4 text) USING csv " +
            "WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
            "partition by column(col1 int4, col2 int4, col3 float8)");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName +
            " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();
    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path(desc.getPath())));
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    Path path = new Path(desc.getPath());
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));

    for (FileStatus partition1 : fs.listStatus(path)){
      assertTrue(fs.isDirectory(partition1.getPath()));
      for (FileStatus partition2 : fs.listStatus(partition1.getPath())) {
        assertTrue(fs.isDirectory(partition2.getPath()));
        for (FileStatus partition3 : fs.listStatus(partition2.getPath())) {
          assertTrue(fs.isDirectory(partition3.getPath()));
          for (FileStatus file : fs.listStatus(partition3.getPath())) {
            CompressionCodec codec = factory.getCodec(file.getPath());
            assertTrue(codec instanceof DeflateCodec);
          }
        }
      }
    }

    res = executeString("select * from " + tableName + " where col2 = 2");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});

    int i = 0;
    while (res.next()) {
      assertEquals(resultRows1.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows1.get(res.getDouble(4))[1], res.getInt(3));
      i++;
    }
    res.close();
    assertEquals(2, i);

    Map<Double, int []> resultRows2 = Maps.newHashMap();
    resultRows2.put(49.0d, new int[]{3, 3});
    resultRows2.put(45.0d, new int[]{3, 2});
    resultRows2.put(38.0d, new int[]{2, 2});

    res = executeString("select * from " + tableName + " where (col1 = 2 or col1 = 3) and col2 >= 2");
    i = 0;
    while(res.next()) {
      assertEquals(resultRows2.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows2.get(res.getDouble(4))[1], res.getInt(3));
      i++;
    }

    res.close();
    assertEquals(3, i);
  }

  @Test
  public final void testColumnPartitionedTableNoMatchedPartition() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableNoMatchedPartition");
    ResultSet res = executeString(
        "create table " + tableName + " (col4 text) USING csv " +
            "WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
            "partition by column(col1 int4, col2 int4, col3 float8)");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName +
            " select l_returnflag , l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();
    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path(desc.getPath())));
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    Path path = new Path(desc.getPath());
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));

    for (FileStatus partition1 : fs.listStatus(path)){
      assertTrue(fs.isDirectory(partition1.getPath()));
      for (FileStatus partition2 : fs.listStatus(partition1.getPath())) {
        assertTrue(fs.isDirectory(partition2.getPath()));
        for (FileStatus partition3 : fs.listStatus(partition2.getPath())) {
          assertTrue(fs.isDirectory(partition3.getPath()));
          for (FileStatus file : fs.listStatus(partition3.getPath())) {
            CompressionCodec codec = factory.getCodec(file.getPath());
            assertTrue(codec instanceof DeflateCodec);
          }
        }
      }
    }

    res = executeString("select * from " + tableName + " where col2 = 9");
    assertFalse(res.next());
    res.close();
  }

  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions1() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableWithSmallerExpressions1");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    ClientProtos.SubmitQueryResponse response = client.executeQuery("insert overwrite into " + tableName
        + " select l_orderkey, l_partkey from lineitem");

    assertTrue(response.hasErrorMessage());
    assertEquals(response.getErrorMessage(), "INSERT has smaller expressions than target columns\n");

    res = executeFile("case14.sql");
    assertResultSet(res, "case14.result");
    res.close();
  }

  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions2() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableWithSmallerExpressions2");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    ClientProtos.SubmitQueryResponse response = client.executeQuery("insert overwrite into " + tableName
        + " select l_returnflag , l_orderkey, l_partkey from lineitem");

    assertTrue(response.hasErrorMessage());
    assertEquals(response.getErrorMessage(), "INSERT has smaller expressions than target columns\n");

    res = executeFile("case15.sql");
    assertResultSet(res, "case15.result");
    res.close();
  }


  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions3() throws Exception {
    ResultSet res = executeString("create database testinsertquery1;");
    res.close();
    res = executeString("create database testinsertquery2;");
    res.close();

    res = executeString("create table testinsertquery1.table1 " +
        "(col1 int4, col2 int4, col3 float8)");
    res.close();

    res = executeString("create table testinsertquery2.table1 " +
        "(col1 int4, col2 int4, col3 float8)");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable("testinsertquery1", "table1"));
    assertTrue(catalog.existsTable("testinsertquery2", "table1"));

    res = executeString("insert overwrite into testinsertquery1.table1 " +
        "select l_orderkey, l_partkey, l_quantity from default.lineitem;");
    res.close();

    TableDesc desc = catalog.getTableDesc("testinsertquery1", "table1");
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    res = executeString("insert overwrite into testinsertquery2.table1 " +
        "select col1, col2, col3 from testinsertquery1.table1;");
    res.close();

    desc = catalog.getTableDesc("testinsertquery2", "table1");
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }
  }

  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions5() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableWithSmallerExpressions5");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 text) partition by column(col2 text) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName + "(col1) select l_returnflag from lineitem");
    res.close();
    res = executeString("select * from " + tableName);
    assertResultSet(res);
    res.close();
  }

  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions6() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testColumnPartitionedTableWithSmallerExpressions6");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 text) partition by column(col2 text) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName + "(col1) select l_returnflag from lineitem where l_orderkey = 1");
    res.close();
    res = executeString("select * from " + tableName);
    assertResultSet(res);
    res.close();
  }

  private MasterPlan getQueryPlan(ResultSet res) {
    QueryId queryId;
    if (res instanceof TajoMemoryResultSet) {
      queryId = ((TajoMemoryResultSet) res).getQueryId();
    } else {
      queryId = ((FetchResultSet) res).getQueryId();
    }

    for (TajoWorker eachWorker : testingCluster.getTajoWorkers()) {
      QueryMasterTask queryMasterTask = eachWorker.getWorkerContext().getQueryMaster().getQueryMasterTask(queryId, true);
      if (queryMasterTask != null) {
        return queryMasterTask.getQuery().getPlan();
      }
    }

    fail("Can't find query from workers" + queryId);
    return null;
  }

  @Test
  public void testScatteredHashShuffle() throws Exception {
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_TABLE_PARTITION_VOLUME.varname, "2");
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.SHUFFLE_HASH_APPENDER_PAGE_VOLUME.varname, "1");
    try {
      KeyValueSet tableOptions = new KeyValueSet();
      tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
      tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

      Schema schema = new Schema();
      schema.addColumn("col1", TajoDataTypes.Type.TEXT);
      schema.addColumn("col2", TajoDataTypes.Type.TEXT);

      List<String> data = new ArrayList<String>();
      int totalBytes = 0;
      Random rand = new Random(System.currentTimeMillis());
      String col2Data = "Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2" +
          "Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2" +
          "Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2Column-2";

      int index = 0;
      while(true) {
        int col1RandomValue = 1;
        String str = col1RandomValue + "|col2-" + index + "-" + col2Data;
        data.add(str);

        totalBytes += str.getBytes().length;

        if (totalBytes > 4 * 1024 * 1024) {
          break;
        }
        index++;
      }

      TajoTestingCluster.createTable("testscatteredhashshuffle", schema, tableOptions, data.toArray(new String[]{}), 3);
      CatalogService catalog = testingCluster.getMaster().getCatalog();
      assertTrue(catalog.existsTable("default", "testscatteredhashshuffle"));

      executeString("create table test_partition (col2 text) partition by column (col1 text)").close();
      executeString("insert into test_partition select col2, col1 from testscatteredhashshuffle").close();

      ResultSet res = executeString("select col1 from test_partition");

      int numRows = 0;
      while (res.next()) {
        numRows++;
      }
      assertEquals(data.size(), numRows);

      // assert data file size

    } finally {
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_TABLE_PARTITION_VOLUME.varname,
          TajoConf.ConfVars.$DIST_QUERY_TABLE_PARTITION_VOLUME.defaultVal);
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.SHUFFLE_HASH_APPENDER_PAGE_VOLUME.varname,
          TajoConf.ConfVars.SHUFFLE_HASH_APPENDER_PAGE_VOLUME.defaultVal);
      executeString("DROP TABLE test_partition PURGE").close();
      executeString("DROP TABLE testScatteredHashShuffle PURGE").close();
    }
  }

  @Test
  public final void TestSpecialCharPartitionKeys1() throws Exception {
    // See - TAJO-947: ColPartitionStoreExec can cause URISyntaxException due to special characters.

    executeDDL("lineitemspecial_ddl.sql", "lineitemspecial.tbl");

    executeString("CREATE TABLE IF NOT EXISTS pTable947 (id int, name text) PARTITION BY COLUMN (type text)")
        .close();
    executeString("INSERT OVERWRITE INTO pTable947 SELECT l_orderkey, l_shipinstruct, l_shipmode FROM lineitemspecial")
        .close();
    ResultSet res = executeString("select * from pTable947 where type='RA:*?><I/L#%S' or type='AIR'");

    String resStr = resultSetToString(res);
    String expected =
        "id,name,type\n" +
            "-------------------------------\n"
            + "3,NONE,AIR\n"
            + "3,TEST SPECIAL CHARS,RA:*?><I/L#%S\n";

    assertEquals(expected, resStr);
    cleanupQuery(res);
  }

  @Test
  public final void TestSpecialCharPartitionKeys2() throws Exception {
    // See - TAJO-947: ColPartitionStoreExec can cause URISyntaxException due to special characters.

    executeDDL("lineitemspecial_ddl.sql", "lineitemspecial.tbl");

    executeString("CREATE TABLE IF NOT EXISTS pTable948 (id int, name text) PARTITION BY COLUMN (type text)")
        .close();
    executeString("INSERT OVERWRITE INTO pTable948 SELECT l_orderkey, l_shipinstruct, l_shipmode FROM lineitemspecial")
        .close();

    ResultSet res = executeString("select * from pTable948 where type='RA:*?><I/L#%S'");
    assertResultSet(res);
    cleanupQuery(res);

    res = executeString("select * from pTable948 where type='RA:*?><I/L#%S' or type='AIR01'");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testIgnoreFilesInIntermediateDir() throws Exception {
    // See - TAJO-1219: Files located in intermediate directories of partitioned table should be ignored
    // It verifies that Tajo ignores files located in intermediate directories of partitioned table.

    Path testDir = CommonTestingUtil.getTestDir();

    executeString(
      "CREATE EXTERNAL TABLE testIgnoreFilesInIntermediateDir (col1 int) USING CSV PARTITION BY COLUMN (col2 text) " +
        "LOCATION '" + testDir + "'");

    FileSystem fs = testDir.getFileSystem(conf);
    FSDataOutputStream fos = fs.create(new Path(testDir, "table1.data"));
    fos.write("a|b|c".getBytes());
    fos.close();

    ResultSet res = executeString("select * from testIgnoreFilesInIntermediateDir;");
    assertFalse(res.next());
    res.close();
  }
}
