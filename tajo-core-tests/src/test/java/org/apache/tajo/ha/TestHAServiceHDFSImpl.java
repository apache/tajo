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

package org.apache.tajo.ha;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertNotEquals;

public class TestHAServiceHDFSImpl  {
  private static final Log LOG = LogFactory.getLog(TestHAServiceHDFSImpl.class);

  private TajoConf conf;
  private TajoTestingCluster util;
  private FileTablespace sm;
  private final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestHAServiceHDFSImpl";
  private CatalogService catalog;
  private Path testDir;

  private final int numTuple = 1000;
  private Random rnd = new Random(System.currentTimeMillis());

  private TableDesc employee;

  private Path haPath, activePath, backupPath;

  @Before
  public void setUp() throws Exception {
    this.conf = new TajoConf();
    util = new TajoTestingCluster(true);

    util.startMaster();
    catalog = util.getCatalogService();

    sm = TablespaceManager.getLocalFs();

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    conf.setVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR, testDir.toString());

    Schema schema = new Schema();
    schema.addColumn("managerid", TajoDataTypes.Type.INT4);
    schema.addColumn("empid", TajoDataTypes.Type.INT4);
    schema.addColumn("deptname", TajoDataTypes.Type.TEXT);

    TableMeta employeeMeta = CatalogUtil.newTableMeta("TEXT");
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs())
      .getAppender(employeeMeta, schema, employeePath);
    appender.enableStats();
    appender.init();
    VTuple tuple = new VTuple(schema.size());
    for (int i = 0; i < numTuple; i++) {
      tuple.put(new Datum[] {
        DatumFactory.createInt4(rnd.nextInt(50)),
        DatumFactory.createInt4(rnd.nextInt(100)),
        DatumFactory.createText("dept_" + i),
      });
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    employee = new TableDesc("default.employee", schema, employeeMeta, employeePath.toUri());
    catalog.createTable(employee);
  }

  @After
  public void tearDown() throws Exception {
    CommonTestingUtil.cleanupTestDir(TEST_PATH);
    util.shutdownCatalogCluster();
  }

  @Test
  public final void testAutoFailOver() throws Exception {
    FileSystem fs = sm.getFileSystem();
    TajoConf primaryConf = util.getConfiguration();
    TajoMaster primaryMaster = util.getMaster();
    assertNotNull(primaryMaster);

    TajoConf backupConf = getBackupMasterConfiguration();
    TajoMaster backupMaster = new TajoMaster();
    backupMaster.init(backupConf);
    backupMaster.start();
    Assert.assertNotNull(backupMaster);

    ServiceTracker tracker = ServiceTrackerFactory.get(primaryConf);
    assertNotEquals(primaryMaster.getMasterName(), backupMaster.getMasterName());

    verifySystemDirectories(fs, primaryConf);

    assertEquals(2, fs.listStatus(activePath).length);
    assertEquals(1, fs.listStatus(backupPath).length);

    assertTrue(fs.exists(new Path(activePath, HAConstants.ACTIVE_LOCK_FILE)));
    assertTrue(fs.exists(new Path(activePath, primaryMaster.getMasterName().replaceAll(":", "_"))));
    assertTrue(fs.exists(new Path(backupPath, backupMaster.getMasterName().replaceAll(":", "_"))));

    createDatabaseAndTable(tracker);
    verifyDataBaseAndTable(tracker);

    primaryMaster.stop();

    verifyDataBaseAndTable(tracker);

    assertTrue(fs.exists(new Path(activePath, HAConstants.ACTIVE_LOCK_FILE)));
    assertTrue(fs.exists(new Path(activePath, backupMaster.getMasterName().replaceAll(":", "_"))));

    assertEquals(2, fs.listStatus(activePath).length);
    assertEquals(0, fs.listStatus(backupPath).length);

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, "employee"));
  }


  private TajoConf getBackupMasterConfiguration() {
    TajoConf conf = util.getConfiguration();

    conf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS,
      "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS,
      "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS,
      "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.CATALOG_ADDRESS,
      "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.TAJO_MASTER_INFO_ADDRESS,
      "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.REST_SERVICE_ADDRESS,
      "localhost:" + NetUtils.getFreeSocketPort());

    conf.setBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE, true);
    conf.setIntVar(TajoConf.ConfVars.TAJO_MASTER_HA_MONITOR_INTERVAL, 1000);

    //Client API service RPC Server
    conf.setIntVar(TajoConf.ConfVars.MASTER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(TajoConf.ConfVars.WORKER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM, 2);

    // Internal RPC Server
    conf.setIntVar(TajoConf.ConfVars.MASTER_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(TajoConf.ConfVars.QUERY_MASTER_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(TajoConf.ConfVars.WORKER_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(TajoConf.ConfVars.CATALOG_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(TajoConf.ConfVars.SHUFFLE_RPC_SERVER_WORKER_THREAD_NUM, 2);

    return conf;
  }

  private void verifySystemDirectories(FileSystem fs, TajoConf conf) throws Exception {
    haPath = TajoConf.getSystemHADir(conf);
    assertTrue(fs.exists(haPath));

    activePath = new Path(haPath, TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);
    assertTrue(fs.exists(activePath));

    backupPath = new Path(haPath, TajoConstants.SYSTEM_HA_BACKUP_DIR_NAME);
    assertTrue(fs.exists(backupPath));
  }

  private void createDatabaseAndTable(ServiceTracker tracker) throws Exception {
    TajoClient client = null;
    try {
      client = new TajoClientImpl(tracker);
      client.executeQuery("CREATE TABLE default.ha_test1 (age int);");
      client.executeQuery("CREATE TABLE default.ha_test2 (age int);");
    } finally {
      IOUtils.cleanup(null, client);
    }
  }

  private void verifyDataBaseAndTable(ServiceTracker tracker) throws Exception {
    TajoClient client = null;
    try {
      client = new TajoClientImpl(tracker);
      Assert.assertTrue(client.existDatabase("default"));
      Assert.assertTrue(client.existTable("default.ha_test1"));
      Assert.assertTrue(client.existTable("default.ha_test2"));
    } finally {
      IOUtils.cleanup(null, client);
    }
  }

}
