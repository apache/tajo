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

import com.google.common.collect.Sets;
import com.google.protobuf.ServiceException;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

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

  private static Path writeTmpTable(String tableName) throws IOException {
    Path tablePath = StorageUtil.concatPath(testDir, tableName);
    BackendTestingUtil.writeTmpTable(conf, tablePath);
    return tablePath;
  }

  @Test
  public final void testKillQuery() throws IOException, ServiceException, InterruptedException {
    ClientProtos.GetQueryStatusResponse res = client.executeQuery("select sleep(1) from lineitem");
    Thread.sleep(1000);
    QueryId queryId = new QueryId(res.getQueryId());
    client.killQuery(queryId);
    assertEquals(TajoProtos.QueryState.QUERY_KILLED, client.getQueryStatus(queryId).getState());
  }

  @Test
  public final void testUpdateQuery() throws IOException, ServiceException {
    final String tableName = "testUpdateQuery";
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
    final String tableName = "testCreateAndDropExternalTableByExecuteQuery";

    Path tablePath = writeTmpTable(tableName);
    assertFalse(client.existTable(tableName));

    String sql = "create external table " + tableName + " (deptname text, score int4) " + "using csv location '"
        + tablePath + "'";

    client.executeQueryAndGetResult(sql);
    assertTrue(client.existTable(tableName));

    client.updateQuery("drop table " + tableName);
    assertFalse(client.existTable(tableName));
    FileSystem localFS = FileSystem.getLocal(conf);
    assertTrue(localFS.exists(tablePath));
  }

  @Test
  public final void testCreateAndPurgeExternalTableByExecuteQuery() throws IOException, ServiceException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndPurgeExternalTableByExecuteQuery";

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
    final String tableName = "testCreateAndDropTableByExecuteQuery";

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
    final String tableName = "testCreateAndPurgeTableByExecuteQuery";

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
    final String tableName = "testDDLByExecuteQuery";
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

    Set<String> tables = Sets.newHashSet(client.getTableList());
    assertTrue(tables.contains(tableName1));
    assertTrue(tables.contains(tableName2));
  }

  Log LOG = LogFactory.getLog(TestTajoClient.class);

  @Test
  public final void testGetTableDesc() throws IOException, ServiceException, SQLException {
    final String tableName1 = "table3";
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
    assertEquals(tableName1, desc.getName());
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
    final String tableName = "testCreateAndDropTablePartitionedColumnByExecuteQuery";

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += "PARTITION BY COLUMN (deptname text)";

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
      if(functionName.equals(eachFunction.getSignature())) {
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
}
