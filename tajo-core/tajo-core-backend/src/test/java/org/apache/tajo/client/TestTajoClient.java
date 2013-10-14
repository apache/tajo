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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BackendTestingUtil;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestTajoClient {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static TajoClient tajo;
  private static String TEST_PATH = "target/test-data/"
      + TestTajoClient.class.getName();
  private static Path testDir;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new TajoTestingCluster();
    cluster.startMiniCluster(1);
    conf = cluster.getConfiguration();
    Thread.sleep(3000);
    tajo = new TajoClient(conf);

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.shutdownMiniCluster();
    if(tajo != null) {
      tajo.close();
    }
  }

  private static Path writeTmpTable(String tableName) throws IOException {
    Path tablePath = StorageUtil.concatPath(testDir, tableName);
    BackendTestingUtil.writeTmpTable(conf, testDir, tableName, true);
    return tablePath;
  }

  @Test
  public final void testAttachTable() throws IOException, ServiceException {
    final String tableName = "attach";
    Path tablePath = writeTmpTable(tableName);
    assertFalse(tajo.existTable(tableName));
    tajo.attachTable(tableName, tablePath);
    assertTrue(tajo.existTable(tableName));
    tajo.detachTable(tableName);
    assertFalse(tajo.existTable(tableName));
  }

  @Test
  public final void testUpdateQuery() throws IOException, ServiceException {
    final String tableName = "testUpdateQuery";
    Path tablePath = writeTmpTable(tableName);

    assertFalse(tajo.existTable(tableName));
    String sql =
        "create external table " + tableName + " (deptname text, score integer) "
            + "using csv location '" + tablePath + "'";
    tajo.updateQuery(sql);
    assertTrue(tajo.existTable(tableName));
  }

  @Test
  public final void testCreateAndDropTable()
      throws IOException, ServiceException {
    final String tableName = "testCreateAndDropTable";
    Path tablePath = writeTmpTable(tableName);

    assertFalse(tajo.existTable(tableName));
    tajo.createTable(tableName, tablePath, BackendTestingUtil.mockupMeta);
    assertTrue(tajo.existTable(tableName));
    tajo.dropTable(tableName);
    assertFalse(tajo.existTable(tableName));
    FileSystem fs = tablePath.getFileSystem(conf);
    assertFalse(fs.exists(tablePath));
  }

  @Test
  public final void testCreateAndDropExternalTableByExecuteQuery() throws IOException, ServiceException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropExternalTableByExecuteQuery";

    BackendTestingUtil.writeTmpTable(conf, CommonTestingUtil.getTestDir(), tableName, false);
    Path tablePath = writeTmpTable(tableName);
    assertFalse(tajo.existTable(tableName));

    String sql = "create external table " + tableName + " (deptname text, score int4) " + "using csv location '"
        + tablePath + "'";

    tajo.executeQueryAndGetResult(sql);
    assertTrue(tajo.existTable(tableName));

    tajo.updateQuery("drop table " + tableName);
    assertFalse(tajo.existTable(tableName));
    FileSystem localFS = FileSystem.getLocal(conf);
    assertFalse(localFS.exists(tablePath));
  }

  @Test
  public final void testCreateAndDropTableByExecuteQuery() throws IOException, ServiceException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testCreateAndDropTableByExecuteQuery";

    assertFalse(tajo.existTable(tableName));

    String sql = "create table " + tableName + " (deptname text, score int4)";

    tajo.updateQuery(sql);
    assertTrue(tajo.existTable(tableName));

    Path tablePath = tajo.getTableDesc(tableName).getPath();
    FileSystem hdfs = tablePath.getFileSystem(conf);
    assertTrue(hdfs.exists(tablePath));

    tajo.updateQuery("drop table " + tableName);
    assertFalse(tajo.existTable(tableName));
    assertFalse(hdfs.exists(tablePath));
  }

  @Test
  public final void testDDLByExecuteQuery() throws IOException, ServiceException {
    TajoConf conf = cluster.getConfiguration();
    final String tableName = "testDDLByExecuteQuery";
    BackendTestingUtil.writeTmpTable(conf, CommonTestingUtil.getTestDir(), tableName, false);

    assertFalse(tajo.existTable(tableName));
    String sql =
        "create external table " + tableName + " (deptname text, score int4) "
            + "using csv location 'file:///tmp/" + tableName + "'";
    tajo.executeQueryAndGetResult(sql);
    assertTrue(tajo.existTable(tableName));
  }

  @Test
  public final void testGetTableList() throws IOException, ServiceException {
    final String tableName1 = "table1";
    final String tableName2 = "table2";
    Path table1Path = writeTmpTable(tableName1);
    Path table2Path = writeTmpTable(tableName2);

    assertFalse(tajo.existTable(tableName1));
    assertFalse(tajo.existTable(tableName2));
    tajo.attachTable(tableName1, table1Path);
    assertTrue(tajo.existTable(tableName1));
    Set<String> tables = Sets.newHashSet(tajo.getTableList());
    assertTrue(tables.contains(tableName1));
    tajo.attachTable(tableName2, table2Path);
    assertTrue(tajo.existTable(tableName2));
    tables = Sets.newHashSet(tajo.getTableList());
    assertTrue(tables.contains(tableName1));
    assertTrue(tables.contains(tableName2));
  }

  @Test
  public final void testGetTableDesc() throws IOException, ServiceException {
    final String tableName1 = "table3";
    Path tablePath = writeTmpTable(tableName1);
    assertFalse(tajo.existTable(tableName1));
    tajo.attachTable(tableName1, tablePath);
    assertTrue(tajo.existTable(tableName1));
    TableDesc desc = tajo.getTableDesc(tableName1);
    assertNotNull(desc);
    assertEquals(tableName1, desc.getName());
    assertTrue(desc.getMeta().getStat().getNumBytes() > 0);
  }
}
