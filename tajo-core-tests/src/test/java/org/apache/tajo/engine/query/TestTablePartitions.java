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
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.exception.ReturnStateUtil;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.querymaster.QueryMasterTask;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.plan.serder.PlanProto.ShuffleType.SCATTERED_HASH_SHUFFLE;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestTablePartitions extends QueryTestCaseBase {

  private NodeType nodeType;

  public TestTablePartitions(NodeType nodeType) throws IOException {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
    this.nodeType = nodeType;
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] {
      //type
      {NodeType.INSERT},
      {NodeType.CREATE_TABLE},
    });
  }

  @Test
  public final void testCreateColumnPartitionedTable() throws Exception {
    ResultSet res;
    String tableName = IdentifierUtil.normalizeIdentifier("testCreateColumnPartitionedTable");
    ClientProtos.SubmitQueryResponse response;
    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8) ");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
      assertEquals(2, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
      assertEquals(3, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

      response = client.executeQuery(
          "insert overwrite into " + tableName + " select l_orderkey, l_partkey, " +
              "l_quantity from lineitem");
    } else {
      response = client.executeQuery(
          "create table " + tableName + "(col1 int4, col2 int4) partition by column(key float8) "
              + " as select l_orderkey, l_partkey, l_quantity from lineitem");
    }

    QueryId queryId = new QueryId(response.getQueryId());
    testingCluster.waitForQuerySubmitted(queryId, 10);
    QueryMasterTask queryMasterTask = testingCluster.getQueryMasterTask(queryId);
    assertNotNull(queryMasterTask);
    TajoClientUtil.waitCompletion(client, queryId);

    MasterPlan plan = queryMasterTask.getQuery().getPlan();
    ExecutionBlock rootEB = plan.getRoot();

    assertEquals(1, plan.getChildCount(rootEB.getId()));

    ExecutionBlock insertEB = plan.getChild(rootEB.getId(), 0);
    assertNotNull(insertEB);

    assertEquals(nodeType, insertEB.getPlan().getType());
    assertEquals(1, plan.getChildCount(insertEB.getId()));

    ExecutionBlock scanEB = plan.getChild(insertEB.getId(), 0);

    List<DataChannel> list = plan.getOutgoingChannels(scanEB.getId());
    assertEquals(1, list.size());
    DataChannel channel = list.get(0);
    assertNotNull(channel);
    assertEquals(SCATTERED_HASH_SHUFFLE, channel.getShuffleType());
    assertEquals(1, channel.getShuffleKeys().length);

    TableDesc tableDesc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"key"},
        tableDesc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testCreateColumnPartitionedTableWithJoin() throws Exception {
    ResultSet res;
    ClientProtos.SubmitQueryResponse response;
    String tableName = IdentifierUtil.normalizeIdentifier("testCreateColumnPartitionedTableWithJoin");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8) ");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
      assertEquals(2, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
      assertEquals(3, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

      response = client.executeQuery(
          "insert overwrite into " + tableName + " select l_orderkey, l_partkey, " +
              "l_quantity from lineitem join orders on l_orderkey = o_orderkey");

    } else {
      response = client.executeQuery("create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8) "
          + " AS select l_orderkey, l_partkey, l_quantity from lineitem join orders on l_orderkey = o_orderkey");
    }

    QueryId queryId = new QueryId(response.getQueryId());
    testingCluster.waitForQuerySubmitted(queryId, 10);
    QueryMasterTask queryMasterTask = testingCluster.getQueryMasterTask(queryId);
    assertNotNull(queryMasterTask);
    TajoClientUtil.waitCompletion(client, queryId);

    MasterPlan plan = queryMasterTask.getQuery().getPlan();
    ExecutionBlock rootEB = plan.getRoot();
    assertEquals(1, plan.getChildCount(rootEB.getId()));

    ExecutionBlock insertEB = plan.getChild(rootEB.getId(), 0);
    assertNotNull(insertEB);
    assertEquals(nodeType, insertEB.getPlan().getType());
    assertEquals(1, plan.getChildCount(insertEB.getId()));

    ExecutionBlock scanEB = plan.getChild(insertEB.getId(), 0);

    List<DataChannel> list = plan.getOutgoingChannels(scanEB.getId());
    assertEquals(1, list.size());
    DataChannel channel = list.get(0);
    assertNotNull(channel);
    assertEquals(SCATTERED_HASH_SHUFFLE, channel.getShuffleType());
    assertEquals(1, channel.getShuffleKeys().length);

    TableDesc tableDesc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"key"},
      tableDesc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testCreateColumnPartitionedTableWithSelectedColumns() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testCreateColumnPartitionedTableWithSelectedColumns");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
      assertEquals(3, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
      assertEquals(4, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

      res = executeString("insert overwrite into " + tableName + " (col1, col2, key) select l_orderkey, " +
        "l_partkey, l_quantity from lineitem");
    } else {
      res = executeString("create table " + tableName + " (col1 int4, col2 int4, null_col int4)"
        + " partition by column(key float8) AS select l_orderkey, l_partkey, null, l_quantity from lineitem");
    }
    res.close();

    TableDesc tableDesc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"key"},
        tableDesc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testColumnPartitionedTableByOneColumn() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableByOneColumn");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString("insert overwrite into " + tableName
        + " (col1, col2, key) select l_orderkey, l_partkey, l_quantity from lineitem");
    } else {
      res = executeString("create table " + tableName + " (col1 int4, col2 int4, null_col int4) "
        + " partition by column(key float8) as select l_orderkey, l_partkey, null, l_quantity from lineitem");
    }
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

    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName,
      new String[]{"key"}, desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
    res.close();
  }

  private void assertPartitionDirectories(TableDesc desc) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(desc.getUri());
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=36.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=49.0")));
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, desc.getStats().getNumRows().intValue());
    }
  }

  @Test
  public final void testQueryCasesOnColumnPartitionedTable() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testQueryCasesOnColumnPartitionedTable");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString(
        "insert overwrite into " + tableName
          + " (col1, col2, key) select l_orderkey, l_partkey, l_quantity from lineitem");
    } else {
      res = executeString("create table " + tableName + " (col1 int4, col2 int4, null_col int4) "
        + " partition by column(key float8) as select l_orderkey, l_partkey, null, l_quantity from lineitem");
    }
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

    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"key"},
      desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
    res.close();
  }

  @Test
  public final void testColumnPartitionedTableByThreeColumns() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableByThreeColumns");

    if (nodeType == NodeType.INSERT) {
      res = testBase.execute(
        "create table " + tableName + " (col4 text) partition by column(col1 int4, col2 int4, col3 float8) ");
      res.close();
      TajoTestingCluster cluster = testBase.getTestingCluster();
      CatalogService catalog = cluster.getMaster().getCatalog();
      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString("insert overwrite into " + tableName
        + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    } else {
      res = executeString( "create table " + tableName + " (col4 text) "
        + " partition by column(col1 int4, col2 int4, col3 float8) as select l_returnflag, l_orderkey, l_partkey, " +
        "l_quantity from lineitem");
    }
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    Path path = new Path(desc.getUri());

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
      assertEquals(8, desc.getStats().getNumRows().intValue());
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

    res = executeString("select * from " + tableName + " WHERE (col1 ='1' or col1 = '100') and col3 > 20");
    String result = resultSetToString(res);
    String expectedResult = "col4,col1,col2,col3\n" +
      "-------------------------------\n" +
      "N,1,1,36.0\n";
    res.close();
    assertEquals(expectedResult, result);

    res = executeString("SELECT col1, col2, col3 FROM " + tableName);
    res.close();

    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"col1", "col2", "col3"},
      desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
    res.close();
  }

  @Test
  public final void testInsertIntoColumnPartitionedTableByThreeColumns() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testInsertIntoColumnPartitionedTableByThreeColumns");

    if (nodeType == NodeType.INSERT) {
      res = testBase.execute(
        "create table " + tableName + " (col4 text) partition by column(col1 int4, col2 int4, col3 float8) ");
      res.close();
      TajoTestingCluster cluster = testBase.getTestingCluster();
      CatalogService catalog = cluster.getMaster().getCatalog();
      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString("insert into " + tableName
        + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    } else {
      res = executeString( "create table " + tableName + " (col4 text)"
        + " partition by column(col1 int4, col2 int4, col3 float8) as select l_returnflag, l_orderkey, l_partkey, " +
        "l_quantity from lineitem");
    }
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);

    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"col1", "col2", "col3"},
      desc.getStats().getNumRows());

    Path path = new Path(desc.getUri());

    FileSystem fs = FileSystem.get(conf);
    verifyDirectoriesForThreeColumns(fs, path, 1);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, desc.getStats().getNumRows().intValue());
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

    // TODO: When inserting into already exists partitioned table, table status need to change correctly.
//    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"col1", "col2", "col3"},
//      desc.getStats().getNumRows());

    path = new Path(desc.getUri());

    verifyDirectoriesForThreeColumns(fs, path, 2);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, desc.getStats().getNumRows().intValue());
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
        "R\n" +
        "\\N\n" +
        "\\N\n" +
        "\\N\n" +
        "\\N\n" +
        "\\N\n" +
        "\\N\n";

    String tableData = getTableFileContents(new Path(desc.getUri()));
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

    verifyDirectoriesForThreeColumns(fs, path, 3);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // TODO: If there is existing another partition directory, we must add its rows number to result row numbers.
      // desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
      // assertEquals(6, desc.getStats().getNumRows().intValue());
    }

    verifyKeptExistingData(res, tableName);

    // insert overwrite empty result to partitioned table
    res = executeString("insert overwrite into " + tableName
      + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem where l_orderkey > 100");
    res.close();

    verifyDirectoriesForThreeColumns(fs, path, 4);
    verifyKeptExistingData(res, tableName);

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  private final void verifyKeptExistingData(ResultSet res, String tableName) throws Exception {
    res = executeString("select * from " + tableName + " where col2 = 1 order by col4, col1, col2, col3");
    String resultSetData = resultSetToString(res);
    res.close();
    String expected = "col4,col1,col2,col3\n" +
      "-------------------------------\n" +
      "N,1,1,17.0\n" +
      "N,1,1,17.0\n" +
      "N,1,1,30.0\n" +
      "N,1,1,36.0\n" +
      "N,1,1,36.0\n";

    assertEquals(expected, resultSetData);
  }

  private final void verifyDirectoriesForThreeColumns(FileSystem fs, Path path, int step) throws Exception {
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));

    if (step == 1 || step == 2) {
      assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    } else {
      assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
      assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=30.0")));
    }

    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));
  }

  @Test
  public final void testColumnPartitionedTableByOneColumnsWithCompression() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableByOneColumnsWithCompression");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col2 int4, col3 float8) USING text " +
          "WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
          "PARTITION BY column(col1 int4)");
      res.close();
      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString(
        "insert overwrite into " + tableName + " select l_partkey, l_quantity, l_orderkey from lineitem");
    } else {
      res = executeString(
        "create table " + tableName + " (col2 int4, col3 float8) USING text " +
          "WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
          "PARTITION BY column(col1 int4) as select l_partkey, l_quantity, l_orderkey from lineitem");
    }
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, desc.getStats().getNumRows().intValue());
    }

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path(desc.getUri())));
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    Path path = new Path(desc.getUri());
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

    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"col1"},
      desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testColumnPartitionedTableByTwoColumnsWithCompression() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableByTwoColumnsWithCompression");

    if (nodeType == NodeType.INSERT) {
      res = executeString("create table " + tableName + " (col3 float8, col4 text) USING text " +
        "WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
        "PARTITION by column(col1 int4, col2 int4)");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString(
        "insert overwrite into " + tableName +
          " select  l_quantity, l_returnflag, l_orderkey, l_partkey from lineitem");
    } else {
      res = executeString("create table " + tableName + " (col3 float8, col4 text) USING text " +
          "WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
          "PARTITION by column(col1 int4, col2 int4) as select  l_quantity, l_returnflag, l_orderkey, " +
        "l_partkey from lineitem");
    }
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, desc.getStats().getNumRows().intValue());
    }

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path(desc.getUri())));
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    Path path = new Path(desc.getUri());
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

    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"col1", "col2"},
        desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testColumnPartitionedTableByThreeColumnsWithCompression() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableByThreeColumnsWithCompression");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col4 text) USING text " +
          "WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
          "partition by column(col1 int4, col2 int4, col3 float8)");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString(
        "insert overwrite into " + tableName +
          " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    } else {
      res = executeString("create table " + tableName + " (col4 text) USING text " +
          "WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
          "partition by column(col1 int4, col2 int4, col3 float8) as select l_returnflag, l_orderkey, l_partkey, " +
        "l_quantity from lineitem");
    }
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, desc.getStats().getNumRows().intValue());
    }

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path(desc.getUri())));
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    Path path = new Path(desc.getUri());
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

    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"col1", "col2", "col3"},
      desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testColumnPartitionedTableNoMatchedPartition() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableNoMatchedPartition");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col4 text) USING text " +
          "WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
          "partition by column(col1 int4, col2 int4, col3 float8)");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString(
        "insert overwrite into " + tableName +
          " select l_returnflag , l_orderkey, l_partkey, l_quantity from lineitem");
    } else {
      res = executeString("create table " + tableName + " (col4 text) USING text " +
          "WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
          "partition by column(col1 int4, col2 int4, col3 float8) as select l_returnflag , l_orderkey, l_partkey, " +
        "l_quantity from lineitem");
    }
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, desc.getStats().getNumRows().intValue());
    }

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path(desc.getUri())));
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    Path path = new Path(desc.getUri());
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

    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"col1", "col2", "col3"},
      desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions1() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableWithSmallerExpressions1");
    res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    ClientProtos.SubmitQueryResponse response = client.executeQuery("insert overwrite into " + tableName
        + " select l_orderkey, l_partkey from lineitem");

    assertTrue(ReturnStateUtil.isError(response.getState()));
    assertEquals(response.getState().getMessage(), "INSERT has smaller expressions than target columns");

    res = executeFile("case14.sql");
    assertResultSet(res, "case14.result");
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"key"},
      desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions2() throws Exception {
    ResultSet res = null;
    ClientProtos.SubmitQueryResponse response = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableWithSmallerExpressions2");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      response = client.executeQuery("insert overwrite into " + tableName
        + " select l_returnflag , l_orderkey, l_partkey from lineitem");

      assertTrue(ReturnStateUtil.isError(response.getState()));
      assertEquals(response.getState().getMessage(), "INSERT has smaller expressions than target columns");

      res = executeFile("case15.sql");
      assertResultSet(res, "case15.result");
      res.close();

      TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
      verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"key"},
        desc.getStats().getNumRows());

      executeString("DROP TABLE " + tableName + " PURGE").close();
    }
  }


  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions3() throws Exception {
    ResultSet res = executeString("create database testinsertquery1;");
    res.close();
    res = executeString("create database testinsertquery2;");
    res.close();

    if (nodeType == NodeType.INSERT) {
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
    } else {
      res = executeString("create table testinsertquery1.table1 " +
        "(col1 int4, col2 int4, col3 float8) as select l_orderkey, l_partkey, l_quantity from default.lineitem;");
      res.close();
    }

    TableDesc desc = catalog.getTableDesc("testinsertquery1", "table1");
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, desc.getStats().getNumRows().intValue());
    }

    if (nodeType == NodeType.INSERT) {
      res = executeString("insert overwrite into testinsertquery2.table1 " +
        "select col1, col2, col3 from testinsertquery1.table1;");
      res.close();
    } else {
      res = executeString("create table testinsertquery2.table1 " +
        "(col1 int4, col2 int4, col3 float8) as select col1, col2, col3 from testinsertquery1.table1;");
      res.close();
    }
    desc = catalog.getTableDesc("testinsertquery2", "table1");
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, desc.getStats().getNumRows().intValue());
    }

    executeString("DROP TABLE testinsertquery1.table1 PURGE").close();
    executeString("DROP TABLE testinsertquery2.table1 PURGE").close();
    executeString("DROP DATABASE testinsertquery1").close();
    executeString("DROP DATABASE testinsertquery2").close();
  }

  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions5() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableWithSmallerExpressions5");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col1 text) partition by column(col2 text) ");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString("insert overwrite into " + tableName + "(col1) select l_returnflag from lineitem");

    } else {
      res = executeString("create table " + tableName + " (col1 text) partition by column(col2 text) " +
        " as select l_returnflag, null from lineitem");
    }
    res.close();
    res = executeString("select * from " + tableName);
    assertResultSet(res);
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"col2"},
      desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testColumnPartitionedTableWithSmallerExpressions6() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testColumnPartitionedTableWithSmallerExpressions6");

    if (nodeType == NodeType.INSERT) {
      res = executeString(
        "create table " + tableName + " (col1 text) partition by column(col2 text) ");
      res.close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString(
        "insert overwrite into " + tableName + "(col1) select l_returnflag from lineitem where l_orderkey = 1");
    } else {
      res = executeString( "create table " + tableName + " (col1 text) partition by column(col2 text) " +
        " as select l_returnflag, null from lineitem where l_orderkey = 1");
    }
    res.close();

    res = executeString("select * from " + tableName);
    assertResultSet(res);
    res.close();

    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"col2"},
      desc.getStats().getNumRows());

    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public void testScatteredHashShuffle() throws Exception {
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_TABLE_PARTITION_VOLUME.varname, "2");
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.SHUFFLE_HASH_APPENDER_PAGE_VOLUME.varname, "1");
    try {
      Schema schema = SchemaBuilder.builder()
          .add("col1", TajoDataTypes.Type.TEXT)
          .add("col2", TajoDataTypes.Type.TEXT)
          .build();

      List<String> data = new ArrayList<>();
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

      TajoTestingCluster.createTable(conf, "testscatteredhashshuffle", schema, data.toArray(new String[data.size()]), 3);
      CatalogService catalog = testingCluster.getMaster().getCatalog();
      assertTrue(catalog.existsTable("default", "testscatteredhashshuffle"));

      if (nodeType == NodeType.INSERT) {
        executeString("create table test_partition (col2 text) partition by column (col1 text)").close();
        executeString("insert into test_partition select col2, col1 from testscatteredhashshuffle").close();
      } else {
        executeString("create table test_partition (col2 text) PARTITION BY COLUMN (col1 text) AS select col2, " +
          "col1 from testscatteredhashshuffle").close();
      }

      ResultSet res = executeString("select col1 from test_partition");

      int numRows = 0;
      while (res.next()) {
        numRows++;
      }
      assertEquals(data.size(), numRows);

      TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, "test_partition");
      verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, "test_partition", new String[]{"col1"},
        desc.getStats().getNumRows());

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

    if (nodeType == NodeType.INSERT) {
      executeString("CREATE TABLE IF NOT EXISTS pTable947 (id int, name text) PARTITION BY COLUMN (type text)")
        .close();
      executeString("INSERT OVERWRITE INTO pTable947 SELECT l_orderkey, l_shipinstruct, l_shipmode FROM lineitemspecial")
        .close();
    } else {
      executeString("CREATE TABLE IF NOT EXISTS pTable947 (id int, name text) PARTITION BY COLUMN (type text)" +
        " AS  SELECT l_orderkey, l_shipinstruct, l_shipmode FROM lineitemspecial")
        .close();
    }

    ResultSet res = executeString("select * from pTable947 where type='RA:*?><I/L#%S' or type='AIR'");

    String resStr = resultSetToString(res);
    String expected =
        "id,name,type\n" +
            "-------------------------------\n"
            + "3,NONE,AIR\n"
            + "3,TEST SPECIAL CHARS,RA:*?><I/L#%S\n";

    assertEquals(expected, resStr);
    cleanupQuery(res);

    executeString("DROP TABLE pTable947 PURGE").close();
  }

  @Test
  public final void TestSpecialCharPartitionKeys2() throws Exception {
    // See - TAJO-947: ColPartitionStoreExec can cause URISyntaxException due to special characters.

    executeDDL("lineitemspecial_ddl.sql", "lineitemspecial.tbl");

    if (nodeType == NodeType.INSERT) {
      executeString("CREATE TABLE IF NOT EXISTS pTable948 (id int, name text) PARTITION BY COLUMN (type text)")
        .close();
      executeString("INSERT OVERWRITE INTO pTable948 SELECT l_orderkey, l_shipinstruct, l_shipmode FROM lineitemspecial")
        .close();
    } else {
      executeString("CREATE TABLE IF NOT EXISTS pTable948 (id int, name text) PARTITION BY COLUMN (type text)" +
        " AS SELECT l_orderkey, l_shipinstruct, l_shipmode FROM lineitemspecial")
        .close();
    }

    ResultSet res = executeString("select * from pTable948 where type='RA:*?><I/L#%S'");
    assertResultSet(res);
    cleanupQuery(res);

    res = executeString("select * from pTable948 where type='RA:*?><I/L#%S' or type='AIR01'");
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE pTable948 PURGE").close();
  }

  @Test
  public final void testIgnoreFilesInIntermediateDir() throws Exception {
    // See - TAJO-1219: Files located in intermediate directories of partitioned table should be ignored
    // It verifies that Tajo ignores files located in intermediate directories of partitioned table.

    if (nodeType == NodeType.INSERT) {
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

  /**
   * Verify added partitions to a table. This would check each partition's directory using record of table.
   *
   *
   * @param databaseName
   * @param tableName
   * @param partitionColumns
   * @param numRows
   * @throws Exception
   */
  private void verifyPartitionDirectoryFromCatalog(String databaseName, String tableName,
                                                   String[] partitionColumns, Long numRows) throws Exception {
    int rowCount = 0;
    FileSystem fs = FileSystem.get(conf);
    Path partitionPath = null;

    // Get all partition column values
    StringBuilder query = new StringBuilder();
    query.append("SELECT");
    for (int i = 0; i < partitionColumns.length; i++) {
      String partitionColumn = partitionColumns[i];
      if (i > 0) {
        query.append(",");
      }
      query.append(" ").append(partitionColumn);
    }
    query.append(" FROM ").append(databaseName).append(".").append(tableName);
    ResultSet res = executeString(query.toString());

    StringBuilder partitionName = new StringBuilder();
    PartitionDescProto partitionDescProto = null;

    // Check whether that partition's directory exist or doesn't exist.
    while(res.next()) {
      partitionName.delete(0, partitionName.length());

      for (int i = 0; i < partitionColumns.length; i++) {
        String partitionColumn = partitionColumns[i];
        if (i > 0) {
          partitionName.append("/");
        }
        String partitionValue = res.getString(partitionColumn);
        if (partitionValue == null) {
          partitionValue = StorageConstants.DEFAULT_PARTITION_NAME;
        }
        partitionName.append(partitionColumn).append("=").append(partitionValue);
      }
      partitionDescProto = catalog.getPartition(databaseName, tableName, partitionName.toString());
      assertNotNull(partitionDescProto);
      assertTrue(partitionDescProto.getPath().indexOf(tableName + "/" + partitionName.toString()) > 0);

      partitionPath = new Path(partitionDescProto.getPath());
      ContentSummary cs = fs.getContentSummary(partitionPath);

      assertEquals(cs.getLength(), partitionDescProto.getNumBytes());
      rowCount++;
    }

    res.close();

    // Check row count.
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(numRows, new Long(rowCount));
    }
  }

  @Test
  public final void testDuplicatedPartitions() throws Exception {
    String tableName = IdentifierUtil.normalizeIdentifier("testDuplicatedPartitions");

    try {
      executeString("CREATE TABLE lineitem2 as select * from lineitem").close();

      // Execute UNION ALL statement for creating multiple output files.
      if (nodeType == NodeType.INSERT) {
        executeString(
          "create table " + tableName + " (col1 int4, col2 int4) partition by column(key text) ").close();

        executeString(
          "insert overwrite into " + tableName
            + " select a.l_orderkey, a.l_partkey, a.l_returnflag from lineitem a union all"
            + " select b.l_orderkey, b.l_partkey, b.l_returnflag from lineitem2 b"
        ).close();
      } else {
        executeString(
          "create table " + tableName + "(col1 int4, col2 int4) partition by column(key text) as "
            + " select a.l_orderkey, a.l_partkey, a.l_returnflag from lineitem a union all"
            + " select b.l_orderkey, b.l_partkey, b.l_returnflag from lineitem2 b"
        ).close();
      }

      // If duplicated partitions had been removed, partitions just will contain 'KEY=N' partition and 'KEY=R'
      // partition. In previous Query and Stage, duplicated partitions were not deleted because they had been in List.
      // If you want to verify duplicated partitions, you need to use List instead of Set with DerbyStore.
      List<PartitionDescProto> partitions = catalog.getPartitionsOfTable(DEFAULT_DATABASE_NAME, tableName);
      assertEquals(3, partitions.size());

      PartitionDescProto firstPartition = catalog.getPartition(DEFAULT_DATABASE_NAME, tableName, "key=N");
      assertNotNull(firstPartition);
      PartitionDescProto secondPartition = catalog.getPartition(DEFAULT_DATABASE_NAME, tableName, "key=R");
      assertNotNull(secondPartition);
    } finally {
      executeString("DROP TABLE lineitem2 PURGE");
      executeString("DROP TABLE " + tableName + " PURGE");
    }
  }

  @Test
  public final void testPatternMatchingPredicatesAndStringFunctions() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testPatternMatchingPredicatesAndStringFunctions");
    String expectedResult;

    if (nodeType == NodeType.INSERT) {
      executeString("create table " + tableName
        + " (col1 int4, col2 int4) partition by column(l_shipdate text, l_returnflag text) ").close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
      assertEquals(2, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
      assertEquals(4, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

      executeString(
        "insert overwrite into " + tableName + " select l_orderkey, l_partkey, l_shipdate, l_returnflag from lineitem");
    } else {
      executeString(
        "create table " + tableName + "(col1 int4, col2 int4) partition by column(l_shipdate text, l_returnflag text) "
          + " as select l_orderkey, l_partkey, l_shipdate, l_returnflag from lineitem");
    }

    assertTrue(client.existTable(tableName));

    // Like
    res = executeString("SELECT * FROM " + tableName
      + " WHERE l_shipdate LIKE '1996%' and l_returnflag = 'N' order by l_shipdate");

    expectedResult = "col1,col2,l_shipdate,l_returnflag\n" +
      "-------------------------------\n" +
      "1,1,1996-03-13,N\n" +
      "1,1,1996-04-12,N\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // Not like
    res = executeString("SELECT * FROM " + tableName
      + " WHERE l_shipdate NOT LIKE '1996%' and l_returnflag IN ('R') order by l_shipdate");

    expectedResult = "col1,col2,l_shipdate,l_returnflag\n" +
      "-------------------------------\n" +
      "3,3,1993-11-09,R\n" +
      "3,2,1994-02-02,R\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // In
    res = executeString("SELECT * FROM " + tableName
      + " WHERE l_shipdate IN ('1993-11-09', '1994-02-02', '1997-01-28') AND l_returnflag = 'R' order by l_shipdate");

    expectedResult = "col1,col2,l_shipdate,l_returnflag\n" +
      "-------------------------------\n" +
      "3,3,1993-11-09,R\n" +
      "3,2,1994-02-02,R\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // Similar to
    res = executeString("SELECT * FROM " + tableName + " WHERE l_shipdate similar to '1993%' order by l_shipdate");

    expectedResult = "col1,col2,l_shipdate,l_returnflag\n" +
      "-------------------------------\n" +
      "3,3,1993-11-09,R\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // Regular expression
    res = executeString("SELECT * FROM " + tableName
      + " WHERE l_shipdate regexp '[1-2][0-9][0-9][3-9]-[0-1][0-9]-[0-3][0-9]' "
      + " AND l_returnflag <> 'N' ORDER BY l_shipdate");

    expectedResult = "col1,col2,l_shipdate,l_returnflag\n" +
      "-------------------------------\n" +
      "3,3,1993-11-09,R\n" +
      "3,2,1994-02-02,R\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // Concatenate
    res = executeString("SELECT * FROM " + tableName
      + " WHERE l_shipdate = ( '1996' || '-' || '03' || '-' || '13' ) order by l_shipdate");

    expectedResult = "col1,col2,l_shipdate,l_returnflag\n" +
      "-------------------------------\n" +
      "1,1,1996-03-13,N\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    executeString("DROP TABLE " + tableName + " PURGE").close();
    res.close();
  }

  @Test
  public final void testDatePartitionColumn() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testDatePartitionColumn");
    String expectedResult;

    if (nodeType == NodeType.INSERT) {
      executeString("create table " + tableName + " (col1 int4, col2 int4) partition by column(key date) ").close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
      assertEquals(2, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
      assertEquals(3, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

      executeString(
        "insert overwrite into " + tableName + " select l_orderkey, l_partkey, l_shipdate from lineitem");
    } else {
      executeString(
        "create table " + tableName + "(col1 int4, col2 int4) partition by column(key date) "
          + " as select l_orderkey, l_partkey, l_shipdate::date from lineitem");
    }

    assertTrue(client.existTable(tableName));

    // LessThanOrEquals
    res = executeString("SELECT * FROM " + tableName + " WHERE key <= date '1995-09-01' order by col1, col2, key");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "3,2,1994-02-02\n" +
      "3,3,1993-11-09\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // LessThan and GreaterThan
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key > to_date('1993-01-01', 'YYYY-MM-DD') " +
      " and key < to_date('1996-01-01', 'YYYY-MM-DD') order by col1, col2, key desc");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "3,2,1994-02-02\n" +
      "3,3,1993-11-09\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // Between
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key between date '1993-01-01' and date '1997-01-01' order by col1, col2, key desc");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,1996-04-12\n" +
      "1,1,1996-03-13\n" +
      "3,2,1994-02-02\n" +
      "3,3,1993-11-09\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // Cast
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key > '1993-01-01'::date " +
      " and key < '1997-01-01'::timestamp order by col1, col2, key ");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,1996-03-13\n" +
      "1,1,1996-04-12\n" +
      "3,2,1994-02-02\n" +
      "3,3,1993-11-09\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // Interval
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key > '1993-01-01'::date " +
      " and key < date '1994-01-01' + interval '1 year' order by col1, col2, key ");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "3,2,1994-02-02\n" +
      "3,3,1993-11-09\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // DateTime Function #1
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key > '1993-01-01'::date " +
      " and key < add_months(date '1994-01-01', 12) order by col1, col2, key ");

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // DateTime Function #2
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key > '1993-01-01'::date " +
      " and key < add_months('1994-01-01'::timestamp, 12) order by col1, col2, key ");

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    executeString("DROP TABLE " + tableName + " PURGE").close();
    res.close();
  }

  @Test
  public final void testTimestampPartitionColumn() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testTimestampPartitionColumn");
    String expectedResult;

    if (nodeType == NodeType.INSERT) {
      executeString("create table " + tableName
        + " (col1 int4, col2 int4) partition by column(key timestamp) ").close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
      assertEquals(2, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
      assertEquals(3, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

      executeString(
        "insert overwrite into " + tableName
          + " select l_orderkey, l_partkey, to_timestamp(l_shipdate, 'YYYY-MM-DD') from lineitem");
    } else {
      executeString(
        "create table " + tableName + "(col1 int4, col2 int4) partition by column(key timestamp) "
          + " as select l_orderkey, l_partkey, to_timestamp(l_shipdate, 'YYYY-MM-DD') from lineitem");
    }

    assertTrue(client.existTable(tableName));

    // LessThanOrEquals
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key <= to_timestamp('1995-09-01', 'YYYY-MM-DD') order by col1, col2, key");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "3,2,1994-02-02 00:00:00\n" +
      "3,3,1993-11-09 00:00:00\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // LessThan and GreaterThan
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key > to_timestamp('1993-01-01', 'YYYY-MM-DD') and " +
      "key < to_timestamp('1996-01-01', 'YYYY-MM-DD') order by col1, col2, key desc");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "3,2,1994-02-02 00:00:00\n" +
      "3,3,1993-11-09 00:00:00\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // Between
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key between to_timestamp('1993-01-01', 'YYYY-MM-DD') " +
      "and to_timestamp('1997-01-01', 'YYYY-MM-DD') order by col1, col2, key desc");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,1996-04-12 00:00:00\n" +
      "1,1,1996-03-13 00:00:00\n" +
      "3,2,1994-02-02 00:00:00\n" +
      "3,3,1993-11-09 00:00:00\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    executeString("DROP TABLE " + tableName + " PURGE").close();
    res.close();
  }

  @Test
  public final void testTimePartitionColumn() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testTimePartitionColumn");
    String  expectedResult;

    if (nodeType == NodeType.INSERT) {
      executeString("create table " + tableName
        + " (col1 int4, col2 int4) partition by column(key time) ").close();

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
      assertEquals(2, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
      assertEquals(3, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

      executeString(
        "insert overwrite into " + tableName
          + " select l_orderkey, l_partkey " +
          " , CASE l_shipdate WHEN '1996-03-13' THEN cast ('11:20:40' as time) " +
          " WHEN '1997-01-28' THEN cast ('12:10:20' as time) " +
          " WHEN '1994-02-02' THEN cast ('12:10:30' as time) " +
          " ELSE cast ('00:00:00' as time) END " +
          " from lineitem");
    } else {
      executeString(
        "create table " + tableName + "(col1 int4, col2 int4) partition by column(key time) "
          + " as select l_orderkey, l_partkey " +
          " , CASE l_shipdate WHEN '1996-03-13' THEN cast ('11:20:40' as time) " +
          " WHEN '1997-01-28' THEN cast ('12:10:20' as time) " +
          " WHEN '1994-02-02' THEN cast ('12:10:30' as time) " +
          " ELSE cast ('00:00:00' as time) END " +
          " from lineitem");
    }

    assertTrue(client.existTable(tableName));
    // LessThanOrEquals
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key <= cast('12:10:20' as time) order by col1, col2, key");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,00:00:00\n" +
      "1,1,11:20:40\n" +
      "2,2,12:10:20\n" +
      "3,3,00:00:00\n" +
        "null,null,00:00:00\n" +
        "null,null,00:00:00\n" +
        "null,null,00:00:00\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // LessThan and GreaterThan
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key > cast('00:00:00' as time) and " +
      "key < cast('12:10:00' as time) order by col1, col2, key desc");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,11:20:40\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    // Between
    res = executeString("SELECT * FROM " + tableName
      + " WHERE key between cast('11:00:00' as time) " +
      "and cast('13:00:00' as time) order by col1, col2, key desc");

    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,11:20:40\n" +
      "2,2,12:10:20\n" +
      "3,2,12:10:30\n";

    assertEquals(expectedResult, resultSetToString(res));
    res.close();

    executeString("DROP TABLE " + tableName + " PURGE").close();
    res.close();
  }

  @Test
  public final void testDatabaseNameIncludeTableName() throws Exception {
    executeString("create database test_partition").close();

    String databaseName = "test_partition";
    String tableName = IdentifierUtil.normalizeIdentifier("part");

    if (nodeType == NodeType.INSERT) {
      executeString(
        "create table " + databaseName + "." + tableName + " (col1 int4, col2 int4) partition by column(key float8) ");

      assertTrue(catalog.existsTable(databaseName, tableName));
      assertEquals(2, catalog.getTableDesc(databaseName, tableName).getSchema().size());
      assertEquals(3, catalog.getTableDesc(databaseName, tableName).getLogicalSchema().size());

      executeString(
        "insert overwrite into " + databaseName + "." + tableName + " select l_orderkey, l_partkey, " +
          "l_quantity from lineitem");
    } else {
      executeString(
        "create table "+ databaseName + "." + tableName + "(col1 int4, col2 int4) partition by column(key float8) "
          + " as select l_orderkey, l_partkey, l_quantity from lineitem");
    }

    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    verifyPartitionDirectoryFromCatalog(databaseName, tableName, new String[]{"key"},
      tableDesc.getStats().getNumRows());

    ResultSet res = executeString("select * from " + databaseName + "." + tableName + " ORDER BY key");

    String result = resultSetToString(res);
    String expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,17.0\n" +
      "1,1,36.0\n" +
      "2,2,38.0\n" +
      "3,2,45.0\n" +
      "3,3,49.0\n" +
        "null,null,null\n" +
        "null,null,null\n" +
        "null,null,null\n";
    res.close();
    assertEquals(expectedResult, result);

    executeString("DROP TABLE " + databaseName + "." + tableName + " PURGE").close();
    executeString("DROP database " + databaseName).close();
  }

  @Test
  public void testAbnormalDirectories()  throws Exception {
    ResultSet res = null;
    FileSystem fs = FileSystem.get(conf);
    Path path = null;

    String tableName = IdentifierUtil.normalizeIdentifier("testAbnormalDirectories");
    if (nodeType == NodeType.INSERT) {
      executeString(
        "create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8) ").close();
      executeString(
        "insert overwrite into " + tableName + " select l_orderkey, l_partkey, " +
          "l_quantity from lineitem").close();
    } else {
      executeString(
        "create table " + tableName + "(col1 int4, col2 int4) partition by column(key float8) "
        + " as select l_orderkey, l_partkey, l_quantity from lineitem").close();
    }

    TableDesc tableDesc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);

    verifyPartitionDirectoryFromCatalog(DEFAULT_DATABASE_NAME, tableName, new String[]{"key"},
      tableDesc.getStats().getNumRows());

    // When partitions only exist on file system without catalog.
    String externalTableName = "testCreateExternalColumnPartitionedTable";

    executeString("create external table " + externalTableName + " (col1 int4, col2 int4) " +
      " USING TEXT WITH ('text.delimiter'='|') PARTITION BY COLUMN (key float8) " +
      " location '" + tableDesc.getUri().getPath() + "'").close();

    res = executeString("SELECT COUNT(*) AS cnt FROM " + externalTableName);
    String result = resultSetToString(res);
    String expectedResult = "cnt\n" +
      "-------------------------------\n" +
      "8\n";
    res.close();
    assertEquals(expectedResult, result);

    // Make abnormal directories
    path = new Path(tableDesc.getUri().getPath(), "key=100.0");
    fs.mkdirs(path);
    path = new Path(tableDesc.getUri().getPath(), "key=");
    fs.mkdirs(path);
    path = new Path(tableDesc.getUri().getPath(), "col1=a");
    fs.mkdirs(path);

    res = executeString("SELECT COUNT(*) AS cnt FROM " + externalTableName + " WHERE key > 40.0");
    result = resultSetToString(res);
    expectedResult = "cnt\n" +
      "-------------------------------\n" +
      "2\n";
    res.close();
    assertEquals(expectedResult, result);

    // Remove existing partition directory
    path = new Path(tableDesc.getUri().getPath(), "key=36.0");
    fs.delete(path, true);

    res = executeString("SELECT * FROM " + tableName + " ORDER BY key");
    result = resultSetToString(res);
    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,17.0\n" +
      "2,2,38.0\n" +
      "3,2,45.0\n" +
      "3,3,49.0\n" +
        "null,null,null\n" +
        "null,null,null\n" +
        "null,null,null\n";
    res.close();
    assertEquals(expectedResult, result);

    res = executeString("SELECT COUNT(*) AS cnt FROM " + tableName + " WHERE key > 30.0");
    result = resultSetToString(res);
    expectedResult = "cnt\n" +
      "-------------------------------\n" +
      "6\n";
    res.close();
    assertEquals(expectedResult, result);

    // Sort
    String sortedTableName = "sortedPartitionTable";
    executeString("create table " + sortedTableName + " AS SELECT * FROM " + tableName
        + " ORDER BY col1, col2 desc").close();

    res = executeString("SELECT * FROM " + sortedTableName + " ORDER BY col1, col2 desc;");
    result = resultSetToString(res);
    expectedResult = "col1,col2,key\n" +
      "-------------------------------\n" +
      "1,1,17.0\n" +
      "2,2,38.0\n" +
      "3,3,49.0\n" +
      "3,2,45.0\n" +
        "null,null,null\n" +
        "null,null,null\n" +
        "null,null,null\n";
    res.close();
    assertEquals(expectedResult, result);

    executeString("DROP TABLE " + sortedTableName + " PURGE").close();
    executeString("DROP TABLE " + externalTableName).close();
    executeString("DROP TABLE " + tableName + " PURGE").close();
  }

  @Test
  public final void testPartitionWithInOperator() throws Exception {
    ResultSet res = null;
    String tableName = IdentifierUtil.normalizeIdentifier("testPartitionWithInOperator");
    String result, expectedResult;

    if (nodeType == NodeType.INSERT) {
      res = testBase.execute(
        "create table " + tableName + " (col4 text) partition by column(col1 int4, col2 int4, col3 float8) ");
      res.close();
      TajoTestingCluster cluster = testBase.getTestingCluster();
      CatalogService catalog = cluster.getMaster().getCatalog();
      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

      res = executeString("insert overwrite into " + tableName
        + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    } else {
      res = executeString( "create table " + tableName + " (col4 text) "
        + " partition by column(col1 int4, col2 int4, col3 float8) as select l_returnflag, l_orderkey, l_partkey, " +
        "l_quantity from lineitem");
    }
    res.close();

    res = executeString("select * from " + tableName);
    result = resultSetToString(res);
    expectedResult = "col4,col1,col2,col3\n" +
      "-------------------------------\n" +
      "N,1,1,17.0\n" +
      "N,1,1,36.0\n" +
      "N,2,2,38.0\n" +
      "R,3,2,45.0\n" +
      "R,3,3,49.0\n" +
        "null,null,null,null\n" +
        "null,null,null,null\n" +
        "null,null,null,null\n";
    res.close();
    assertEquals(expectedResult, result);

    res = executeString("select * from " + tableName + " WHERE col1 in (1, 2) order by col3");
    result = resultSetToString(res);
    expectedResult = "col4,col1,col2,col3\n" +
      "-------------------------------\n" +
      "N,1,1,17.0\n" +
      "N,1,1,36.0\n" +
      "N,2,2,38.0\n";
    res.close();
    assertEquals(expectedResult, result);

    res = executeString("select * from " + tableName + " WHERE col1 in (2, 3) and col2 = 2 order by col3");
    result = resultSetToString(res);
    expectedResult = "col4,col1,col2,col3\n" +
      "-------------------------------\n" +
      "N,2,2,38.0\n" +
      "R,3,2,45.0\n";
    res.close();
    assertEquals(expectedResult, result);

    res = executeString("select * from " + tableName + " WHERE col1 in (1, 2, 3) and col2 in (1, 3) and col3 < 30 " +
      " order by col3");
    result = resultSetToString(res);
    expectedResult = "col4,col1,col2,col3\n" +
      "-------------------------------\n" +
      "N,1,1,17.0\n";
    res.close();
    assertEquals(expectedResult, result);

    executeString("DROP TABLE " + tableName + " PURGE").close();
    res.close();
  }

}
