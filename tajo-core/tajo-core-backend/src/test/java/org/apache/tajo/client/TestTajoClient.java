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
import org.apache.tajo.BackendTestingUtil;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.SQLException;
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
  public final void testUpdateQuery() throws IOException, ServiceException {
    final String tableName = "testUpdateQuery";
    Path tablePath = writeTmpTable(tableName);

    assertFalse(client.existTable(tableName));
    String sql =
        "create external table " + tableName + " (deptname text, score integer) "
            + "using csv location '" + tablePath + "'";
    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));
    client.detachTable(tableName);
    assertFalse(client.existTable(tableName));
  }

  @Test
  public final void testCreateAndDropTable()
      throws IOException, ServiceException, SQLException {
    final String tableName = "testCreateAndDropTable";
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
    assertFalse(fs.exists(tablePath));
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
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testDDLByExecuteQuery() throws IOException, ServiceException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testDDLByExecuteQuery";
    BackendTestingUtil.writeTmpTable(conf, CommonTestingUtil.getTestDir());

    assertFalse(client.existTable(tableName));
    String sql =
        "create external table " + tableName + " (deptname text, score int4) "
            + "using csv location 'file:///tmp/" + tableName + "'";
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

  @Test
  public final void testCreateAndDropTablePartitionedHash1ByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTablePartitionedHash1";

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
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testCreateAndDropTablePartitionedHash2ByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTablePartitionedHash2";

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += "PARTITION BY HASH (deptname)";
    sql += "PARTITIONS 2";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName);
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testCreateAndDropTablePartitionedListByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTablePartitionedList";

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

    client.updateQuery("drop table " + tableName);
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testCreateAndDropTablePartitionedRangeByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTablePartitionedRange";

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

    client.updateQuery("drop table " + tableName);
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }
  @Test
  public final void testCreateAndDropTablePartitionedColumnByExecuteQuery() throws IOException,
      ServiceException, SQLException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTablePartitionedColumn";

    assertFalse(client.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";
    sql += "PARTITION BY COLUMN (deptname)";

    client.updateQuery(sql);
    assertTrue(client.existTable(tableName));

    Path tablePath = client.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    client.updateQuery("drop table " + tableName);
    assertFalse(client.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

}
