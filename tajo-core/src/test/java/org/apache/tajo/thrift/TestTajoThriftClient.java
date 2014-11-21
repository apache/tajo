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

package org.apache.tajo.thrift;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.thrift.client.TajoThriftClient;
import org.apache.tajo.thrift.client.TajoThriftResultSet;
import org.apache.tajo.thrift.generated.TBriefQueryInfo;
import org.apache.tajo.thrift.generated.TGetQueryStatusResponse;
import org.apache.tajo.thrift.generated.TTableDesc;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class TestTajoThriftClient {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static TajoThriftClient client;
  private static Path testDir;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    conf = new TajoConf(cluster.getConfiguration());
    conf.set(ThriftServerConstants.SERVER_LIST_CONF_KEY, cluster.getThriftServer().getContext().getServerName());
    client = new TajoThriftClient(conf);
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
  public final void testCreateAndDropDatabases() throws Exception {
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
  public final void testCurrentDatabase() throws Exception {
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
  public final void testSelectDatabaseToInvalidOne() throws Exception {
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
  public final void testDropCurrentDatabase() throws Exception {
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
  public final void testSessionVariables() throws Exception {
    String prefixName = "key_";
    String prefixValue = "val_";

    List<String> unsetList = new ArrayList<String>();
    for(Map.Entry<String, String> entry: client.getAllSessionVariables().entrySet()) {
      client.unsetSessionVariable(entry.getKey());
    }

    for (int i = 0; i < 10; i++) {
      String key = prefixName + i;
      String val = prefixValue + i;

      // Basically,
      assertEquals(i + 4, client.getAllSessionVariables().size());
      assertFalse(client.getAllSessionVariables().containsKey(key));
      assertFalse(client.existSessionVariable(key));

      client.updateSessionVariable(key, val);

      assertEquals(i + 5, client.getAllSessionVariables().size());
      assertTrue(client.getAllSessionVariables().containsKey(key));
      assertTrue(client.existSessionVariable(key));
    }

    int totalSessionVarNum = client.getAllSessionVariables().size();

    for (int i = 0; i < 10; i++) {
      String key = prefixName + i;

      assertTrue(client.getAllSessionVariables().containsKey(key));
      assertTrue(client.existSessionVariable(key));

      client.unsetSessionVariable(key);

      assertFalse(client.getAllSessionVariables().containsKey(key));
      assertFalse(client.existSessionVariable(key));
    }

    assertEquals(totalSessionVarNum - 10, client.getAllSessionVariables().size());
  }

  @Test
  public final void testKillQuery() throws Exception {
    TGetQueryStatusResponse res = client.executeQuery("select sleep(2) from lineitem");
    Thread.sleep(1000);
    client.killQuery(res.getQueryId());
    Thread.sleep(2000);
    assertEquals(QueryState.QUERY_KILLED.name(), client.getQueryStatus(res.getQueryId()).getState());
  }

  @Test
  public final void testUpdateQuery() throws Exception {
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
  public final void testCreateAndDropTableByExecuteQuery() throws Exception {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = CatalogUtil.normalizeIdentifier("testCreateAndDropTableByExecuteQuery");

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = new Path(client.getTableDesc(tableName).getPath());
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName);
    assertFalse(client.existTable(tableName));
    assertTrue(hdfs.exists(tablePath));
  }

  @Test
  public final void testCreateAndPurgeTableByExecuteQuery() throws Exception {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = CatalogUtil.normalizeIdentifier("testCreateAndPurgeTableByExecuteQuery");

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = new Path(client.getTableDesc(tableName).getPath());
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName + " purge");
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testDDLByExecuteQuery() throws Exception {
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
  public final void testGetTableList() throws Exception {
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

  @Test
  public final void testGetTableDesc() throws Exception {
    TTableDesc desc = client.getTableDesc("default.lineitem");
    assertNotNull(desc);
    assertEquals(CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "lineitem"), desc.getTableName());
    assertTrue(desc.getStats().getNumBytes() > 0);
  }

  @Test
  public final void testFailCreateTablePartitionedOtherExceptColumn() throws Exception {
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
  public final void testCreateAndDropTablePartitionedColumnByExecuteQuery() throws Exception {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = CatalogUtil.normalizeIdentifier("testCreateAndDropTablePartitionedColumnByExecuteQuery");

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += "PARTITION BY COLUMN (key1 text)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = new Path(client.getTableDesc(tableName).getPath());
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName + " purge");
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testGetFinishedQueryList() throws Exception {
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
    TGetQueryStatusResponse response = client.executeQuery(sql);

    assertNotNull(response);
    String queryId = response.getQueryId();

    try {
      long startTime = System.currentTimeMillis();
      while (true) {
        Thread.sleep(5 * 1000);

        List<TBriefQueryInfo> finishedQueries = client.getFinishedQueryList();
        boolean finished = false;
        if (finishedQueries != null) {
          for (TBriefQueryInfo eachQuery: finishedQueries) {
            if (eachQuery.getQueryId().equals(queryId)) {
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

      response = client.getQueryStatus(queryId);
      assertNotNull(response);
      assertTrue(TajoClientUtil.isQueryComplete(TajoProtos.QueryState.valueOf(response.getState())));

      TajoResultSetBase resultSet = (TajoResultSetBase) client.getQueryResult(queryId);
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

    client.updateSessionVariable(SessionVars.NULL_CHAR.keyname(), "\\\\T");
    assertEquals("\\\\T", client.getSessionVariable(SessionVars.NULL_CHAR.keyname()));

    TajoThriftResultSet res = (TajoThriftResultSet)client.executeQueryAndGetResult(sql);

    assertEquals("\\\\T", res.getTableDesc().getTableMeta().get(StorageConstants.TEXT_NULL));

    Path path = new Path(res.getTableDesc().getPath());
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
