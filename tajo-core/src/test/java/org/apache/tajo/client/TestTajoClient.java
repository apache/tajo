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

package org.apache.tajo.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ServiceException;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.jdbc.TajoResultSet;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestTajoClient {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static TajoClient client;
  private static Path testDir;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    conf = cluster.getConfiguration();
    client = new TajoClient(conf);
    testDir = CommonTestingUtil.getTestDir();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.close();
  }

  private static Path writeTmpTable(String tableName) throws IOException {
    Path tablePath = StorageUtil.concatPath(testDir, tableName);
    BackendTestingUtil.writeTmpTable(conf, tablePath);
    return tablePath;
  }

  @Test
  public final void testCreateAndDropDatabases() throws ServiceException {
    int currentNum = client.getAllDatabaseNames().size();

    String prefix = CatalogUtil.normalizeIdentifier("testCreateDatabase_");
    for (int i = 0; i < 10; i++) {
      // test allDatabaseNames
      assertEquals(currentNum + i, client.getAllDatabaseNames().size());

      // test existence
      assertFalse(client.existDatabase(prefix + i));
      assertTrue(client.createDatabase(prefix + i));
      assertTrue(client.existDatabase(prefix + i));

      // test allDatabaseNames
      assertEquals(currentNum + i + 1, client.getAllDatabaseNames().size());
      assertTrue(client.getAllDatabaseNames().contains(prefix + i));
    }

    // test dropDatabase, existDatabase and getAllDatabaseNames()
    for (int i = 0; i < 10; i++) {
      assertTrue(client.existDatabase(prefix + i));
      assertTrue(client.getAllDatabaseNames().contains(prefix + i));
      assertTrue(client.dropDatabase(prefix + i));
      assertFalse(client.existDatabase(prefix + i));
      assertFalse(client.getAllDatabaseNames().contains(prefix + i));
    }

    assertEquals(currentNum, client.getAllDatabaseNames().size());
  }

  @Test
  public final void testCurrentDatabase() throws IOException, ServiceException, InterruptedException {
    int currentNum = client.getAllDatabaseNames().size();
    assertEquals(TajoConstants.DEFAULT_DATABASE_NAME, client.getCurrentDatabase());

    String databaseName = CatalogUtil.normalizeIdentifier("testcurrentdatabase");
    assertTrue(client.createDatabase(databaseName));
    assertEquals(currentNum + 1, client.getAllDatabaseNames().size());
    assertEquals(TajoConstants.DEFAULT_DATABASE_NAME, client.getCurrentDatabase());
    assertTrue(client.selectDatabase(databaseName));
    assertEquals(databaseName, client.getCurrentDatabase());
    assertTrue(client.selectDatabase(TajoConstants.DEFAULT_DATABASE_NAME));
    assertTrue(client.dropDatabase(databaseName));

    assertEquals(currentNum, client.getAllDatabaseNames().size());
  }

  @Test
  public final void testSelectDatabaseToInvalidOne() throws IOException, ServiceException, InterruptedException {
    int currentNum = client.getAllDatabaseNames().size();
    assertFalse(client.existDatabase("invaliddatabase"));

    try {
      assertTrue(client.selectDatabase("invaliddatabase"));
      assertFalse(true);
    } catch (Throwable t) {
      assertFalse(false);
    }

    assertEquals(currentNum, client.getAllDatabaseNames().size());
  }

  @Test
  public final void testDropCurrentDatabase() throws IOException, ServiceException, InterruptedException {
    int currentNum = client.getAllDatabaseNames().size();
    String databaseName = CatalogUtil.normalizeIdentifier("testdropcurrentdatabase");
    assertTrue(client.createDatabase(databaseName));
    assertTrue(client.selectDatabase(databaseName));
    assertEquals(databaseName, client.getCurrentDatabase());

    try {
      client.dropDatabase(databaseName);
      assertFalse(true);
    } catch (Throwable t) {
      assertFalse(false);
    }

    assertTrue(client.selectDatabase(TajoConstants.DEFAULT_DATABASE_NAME));
    assertTrue(client.dropDatabase(databaseName));
    assertEquals(currentNum, client.getAllDatabaseNames().size());
  }

  @Test
  public final void testSessionVariables() throws IOException, ServiceException, InterruptedException {
    String prefixName = "key_";
    String prefixValue = "val_";

    List<String> unsetList = new ArrayList<String>();
    for(Map.Entry<String, String> entry: client.getAllSessionVariables().entrySet()) {
      unsetList.add(entry.getKey());
    }
    client.unsetSessionVariables(unsetList);

    for (int i = 0; i < 10; i++) {
      String key = prefixName + i;
      String val = prefixValue + i;

      // Basically,
      assertEquals(i + 4, client.getAllSessionVariables().size());
      assertFalse(client.getAllSessionVariables().containsKey(key));
      assertFalse(client.existSessionVariable(key));

      Map<String, String> map = Maps.newHashMap();
      map.put(key, val);
      client.updateSessionVariables(map);

      assertEquals(i + 5, client.getAllSessionVariables().size());
      assertTrue(client.getAllSessionVariables().containsKey(key));
      assertTrue(client.existSessionVariable(key));
    }

    int totalSessionVarNum = client.getAllSessionVariables().size();

    for (int i = 0; i < 10; i++) {
      String key = prefixName + i;

      assertTrue(client.getAllSessionVariables().containsKey(key));
      assertTrue(client.existSessionVariable(key));

      client.unsetSessionVariables(Lists.newArrayList(key));

      assertFalse(client.getAllSessionVariables().containsKey(key));
      assertFalse(client.existSessionVariable(key));
    }

    assertEquals(totalSessionVarNum - 10, client.getAllSessionVariables().size());
  }

  @Test
  public final void testKillQuery() throws IOException, ServiceException, InterruptedException {
    ClientProtos.SubmitQueryResponse res = client.executeQuery("select sleep(1) from lineitem");
    Thread.sleep(1000);
    QueryId queryId = new QueryId(res.getQueryId());
    client.killQuery(queryId);
    assertEquals(TajoProtos.QueryState.QUERY_KILLED, client.getQueryStatus(queryId).getState());
  }

  @Test
  public final void testUpdateQuery() throws IOException, ServiceException {
    final String tableName = CatalogUtil.normalizeIdentifier("testUpdateQuery");
    Path tablePath = writeTmpTable(tableName);

    assertFalse(client.existTable(tableName));
    String sql =
        "create external table " + tableName + " (deptname text, score integer) "
            + "using csv location '" + tablePath + "'";
    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));
    client.dropTable(tableName);
    assertFalse(client.existTable(tableName));
  }

  @Test
  public final void testCreateAndDropExternalTable()
      throws IOException, ServiceException, SQLException {
    final String tableName = "testCreateAndDropExternalTable";
    Path tablePath = writeTmpTable(tableName);
    LOG.error("Full path:" + tablePath.toUri().getRawPath());
    FileSystem fs = tablePath.getFileSystem(conf);
    assertTrue(fs.exists(tablePath));

    assertFalse(client.existTable(tableName));

    client.createExternalTable(tableName, BackendTestingUtil.mockupSchema, tablePath, BackendTestingUtil.mockupMeta);
    assertTrue(client.existTable(tableName));
    client.dropTable(tableName);
    assertFalse(client.existTable(tableName));
    fs = tablePath.getFileSystem(conf);
    assertTrue(fs.exists(tablePath));
  }

  @Test
  public final void testCreateAndPurgeExternalTable() throws IOException, ServiceException, SQLException {
    final String tableName = "testCreateAndPurgeExternalTable";
    Path tablePath = writeTmpTable(tableName);
    LOG.error("Full path:" + tablePath.toUri().getRawPath());
    FileSystem fs = tablePath.getFileSystem(conf);
    assertTrue(fs.exists(tablePath));

    assertFalse(client.existTable(tableName));

    client.createExternalTable(tableName, BackendTestingUtil.mockupSchema, tablePath, BackendTestingUtil.mockupMeta);
    assertTrue(client.existTable(tableName));
    client.dropTable(tableName, true);
    assertFalse(client.existTable(tableName));
    fs = tablePath.getFileSystem(conf);
    assertFalse("Checking if table data exists", fs.exists(tablePath));
  }

  @Test
  public final void testCreateAndDropExternalTableByExecuteQuery() throws IOException, ServiceException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = CatalogUtil.normalizeIdentifier("testCreateAndDropExternalTableByExecuteQuery");

    Path tablePath = writeTmpTable(tableName);
    assertFalse(client.existTable(tableName));

    String sql = "create external table " + tableName + " (deptname text, score int4) " + "using csv location '"
        + tablePath + "'";

    client.executeQueryAndGetResult(sql);
    assertTrue(client.existTable(tableName));

    client.updateQuery("drop table " + tableName);
    assertFalse(client.existTable("tableName"));
    FileSystem localFS = FileSystem.getLocal(conf);
    assertTrue(localFS.exists(tablePath));
  }

  @Test
  public final void testCreateAndPurgeExternalTableByExecuteQuery() throws IOException, ServiceException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = CatalogUtil.normalizeIdentifier("testCreateAndPurgeExternalTableByExecuteQuery");

    Path tablePath = writeTmpTable(tableName);
    assertFalse(client.existTable(tableName));

    String sql = "create external table " + tableName + " (deptname text, score int4) " + "using csv location '"
        + tablePath + "'";

    client.executeQueryAndGetResult(sql);
    assertTrue(client.existTable(tableName));

    client.updateQuery("drop table " + tableName + " purge");
    assertFalse(client.existTable(tableName));
    FileSystem localFS = FileSystem.getLocal(conf);
    assertFalse(localFS.exists(tablePath));
  }

  @Test
  public final void testCreateAndDropTableByExecuteQuery() throws IOException, ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = CatalogUtil.normalizeIdentifier("testCreateAndDropTableByExecuteQuery");

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName);
    assertFalse(client.existTable(tableName));
    assertTrue(hdfs.exists(tablePath));
  }

  @Test
  public final void testCreateAndPurgeTableByExecuteQuery() throws IOException, ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = CatalogUtil.normalizeIdentifier("testCreateAndPurgeTableByExecuteQuery");

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName + " purge");
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testDDLByExecuteQuery() throws IOException, ServiceException {
    final String tableName = CatalogUtil.normalizeIdentifier("testDDLByExecuteQuery");
    Path tablePath = writeTmpTable(tableName);

    assertFalse(client.existTable(tableName));
    String sql =
        "create external table " + tableName + " (deptname text, score int4) "
            + "using csv location '" + tablePath + "'";
    client.executeQueryAndGetResult(sql);
    assertTrue(client.existTable(tableName));
  }

  @Test
  public final void testGetTableList() throws IOException, ServiceException {
    String tableName1 = "GetTableList1".toLowerCase();
    String tableName2 = "GetTableList2".toLowerCase();

    assertFalse(client.existTable(tableName1));
    assertFalse(client.existTable(tableName2));
    client.updateQuery("create table GetTableList1 (age int, name text);");
    client.updateQuery("create table GetTableList2 (age int, name text);");

    assertTrue(client.existTable(tableName1));
    assertTrue(client.existTable(tableName2));

    Set<String> tables = Sets.newHashSet(client.getTableList(null));
    assertTrue(tables.contains(tableName1));
    assertTrue(tables.contains(tableName2));
  }

  Log LOG = LogFactory.getLog(TestTajoClient.class);

  @Test
  public final void testGetTableDesc() throws IOException, ServiceException, SQLException {
    final String tableName1 = CatalogUtil.normalizeIdentifier("table3");
    Path tablePath = writeTmpTable(tableName1);
    LOG.error("Full path:" + tablePath.toUri().getRawPath());
    FileSystem fs = tablePath.getFileSystem(conf);
    assertTrue(fs.exists(tablePath));

    assertNotNull(tablePath);
    assertFalse(client.existTable(tableName1));

    client.createExternalTable("table3", BackendTestingUtil.mockupSchema, tablePath, BackendTestingUtil.mockupMeta);
    assertTrue(client.existTable(tableName1));

    TableDesc desc = client.getTableDesc(tableName1);
    assertNotNull(desc);
    assertEquals(CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, tableName1), desc.getName());
    assertTrue(desc.getStats().getNumBytes() > 0);
  }

  //@Test
  public final void testCreateAndDropTablePartitionedHash1ByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTablePartitionedHash1ByExecuteQuery";

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += " PARTITION BY HASH (deptname)";
    sql += " (PARTITION sub_part1, PARTITION sub_part2, PARTITION sub_part3)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName);
    assertFalse(client.existTable(tableName));
    assertTrue(hdfs.exists(tablePath));
  }

  //@Test
  public final void testCreateAndPurgeTablePartitionedHash1ByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndPurgeTablePartitionedHash1ByExecuteQuery";

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += " PARTITION BY HASH (deptname)";
    sql += " (PARTITION sub_part1, PARTITION sub_part2, PARTITION sub_part3)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName + " purge");
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  //@Test
  public final void testCreateAndDropTablePartitionedHash2ByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTablePartitionedHash2ByExecuteQuery";

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += "PARTITION BY HASH (deptname)";
    sql += "PARTITIONS 2";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName + " purge");
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  //@Test
  public final void testCreateAndDropTablePartitionedListByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTablePartitionedListByExecuteQuery";

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += "PARTITION BY LIST (deptname)";
    sql += "( PARTITION sub_part1 VALUES('r&d', 'design'),";
    sql += "PARTITION sub_part2 VALUES('sales', 'hr') )";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName + " purge");
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  //@Test
  public final void testCreateAndDropTablePartitionedRangeByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTablePartitionedRangeByExecuteQuery";

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += "PARTITION BY RANGE (score)";
    sql += "( PARTITION sub_part1 VALUES LESS THAN (2),";
    sql += "PARTITION sub_part2 VALUES LESS THAN (5),";
    sql += "PARTITION sub_part2 VALUES LESS THAN (MAXVALUE) )";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName +" purge");
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testFailCreateTablePartitionedOtherExceptColumn() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testFailCreateTablePartitionedOtherExceptColumn";

    assertFalse(client.existTable(tableName));

    String rangeSql = "create table " + tableName + " (deptname text, score int4)";
    rangeSql += "PARTITION BY RANGE (score)";
    rangeSql += "( PARTITION sub_part1 VALUES LESS THAN (2),";
    rangeSql += "PARTITION sub_part2 VALUES LESS THAN (5),";
    rangeSql += "PARTITION sub_part2 VALUES LESS THAN (MAXVALUE) )";

    assertFalse(client.updateQuery(rangeSql));
 
    String listSql = "create table " + tableName + " (deptname text, score int4)";
    listSql += "PARTITION BY LIST (deptname)";
    listSql += "( PARTITION sub_part1 VALUES('r&d', 'design'),";
    listSql += "PARTITION sub_part2 VALUES('sales', 'hr') )";

    assertFalse(client.updateQuery(listSql));

    String hashSql = "create table " + tableName + " (deptname text, score int4)";
    hashSql += "PARTITION BY HASH (deptname)";
    hashSql += "PARTITIONS 2";

    assertFalse(client.updateQuery(hashSql));
  }

  @Test
  public final void testCreateAndDropTablePartitionedColumnByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = CatalogUtil.normalizeIdentifier("testCreateAndDropTablePartitionedColumnByExecuteQuery");

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += "PARTITION BY COLUMN (key1 text)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName + " purge");
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testGetFunctions() throws IOException,
      ServiceException, SQLException {
    Collection<FunctionDesc> catalogFunctions = cluster.getMaster().getCatalog().getFunctions();
    String functionName = "sum";
    int numFunctions = 0;
    for(FunctionDesc eachFunction: catalogFunctions) {
      if(functionName.equals(eachFunction.getFunctionName())) {
        numFunctions++;
      }
    }

    List<CatalogProtos.FunctionDescProto> functions = client.getFunctions(functionName);
    assertEquals(numFunctions, functions.size());

    functions = client.getFunctions("notmatched");
    assertEquals(0, functions.size());

    functions = client.getFunctions(null);
    assertEquals(catalogFunctions.size(), functions.size());
  }

  @Test
  public final void testGetFinishedQueryList() throws IOException,
      ServiceException, SQLException {
    final String tableName = CatalogUtil.normalizeIdentifier("testGetFinishedQueryList");
    String sql = "create table " + tableName + " (deptname text, score int4)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    int numFinishedQueries = client.getFinishedQueryList().size();
    ResultSet resultSet = client.executeQueryAndGetResult("select * from " + tableName + " order by deptname");
    assertNotNull(resultSet);

    resultSet = client.executeQueryAndGetResult("select * from " + tableName + " order by deptname");
    assertNotNull(resultSet);
    assertEquals(numFinishedQueries + 2, client.getFinishedQueryList().size());

    resultSet.close();
  }

  /**
   * The main objective of this test is to get the status of a query which is actually finished.
   * Statuses of queries regardless of its status should be available for a specified time duration.
   */
  @Test(timeout = 20 * 1000)
  public final void testGetQueryStatusAndResultAfterFinish() throws Exception {
    String sql = "select * from lineitem order by l_orderkey";
    ClientProtos.SubmitQueryResponse response = client.executeQuery(sql);

    assertNotNull(response);
    QueryId queryId = new QueryId(response.getQueryId());

    try {
      long startTime = System.currentTimeMillis();
      while (true) {
        Thread.sleep(5 * 1000);

        List<ClientProtos.BriefQueryInfo> finishedQueries = client.getFinishedQueryList();
        boolean finished = false;
        if (finishedQueries != null) {
          for (ClientProtos.BriefQueryInfo eachQuery: finishedQueries) {
            if (eachQuery.getQueryId().equals(queryId.getProto())) {
              finished = true;
              break;
            }
          }
        }

        if (finished) {
          break;
        }
        if(System.currentTimeMillis() - startTime > 20 * 1000) {
          fail("Too long time execution query");
        }
      }

      QueryStatus queryStatus = client.getQueryStatus(queryId);
      assertNotNull(queryStatus);
      assertTrue(TajoClient.isInCompleteState(queryStatus.getState()));

      TajoResultSet resultSet = (TajoResultSet) client.getQueryResult(queryId);
      assertNotNull(resultSet);

      int count = 0;
      while(resultSet.next()) {
        count++;
      }

      assertEquals(5, count);
    } finally {
      client.closeQuery(queryId);
    }
  }

  @Test
  public void testSetCvsNull() throws Exception {
    String sql =
        "select\n" +
            "  c_custkey,\n" +
            "  orders.o_orderkey,\n" +
            "  orders.o_orderstatus \n" +
            "from\n" +
            "  orders full outer join customer on c_custkey = o_orderkey\n" +
            "order by\n" +
            "  c_custkey,\n" +
            "  orders.o_orderkey;\n";

    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();

    Map<String, String> variables = new HashMap<String, String>();
    variables.put(SessionVars.NULL_CHAR.keyname(), "\\\\T");
    client.updateSessionVariables(variables);

    TajoResultSet res = (TajoResultSet)client.executeQueryAndGetResult(sql);

    assertEquals(res.getTableDesc().getMeta().getOption(StorageConstants.CSVFILE_NULL), "\\\\T");

    Path path = res.getTableDesc().getPath();
    FileSystem fs = path.getFileSystem(tajoConf);

    FileStatus[] files = fs.listStatus(path);
    assertNotNull(files);
    assertEquals(1, files.length);

    InputStream in = fs.open(files[0].getPath());
    byte[] buf = new byte[1024];


    int readBytes = in.read(buf);
    assertTrue(readBytes > 0);

    // text type field's value is replaced with \T
    String expected = "1|1|O\n" +
        "2|2|O\n" +
        "3|3|F\n" +
        "4||\\T\n" +
        "5||\\T\n";

    String resultDatas = new String(buf, 0, readBytes);

    assertEquals(expected, resultDatas);
  }
}
