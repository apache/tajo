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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertNotEquals;

public class TestHAServiceHDFSImpl  {
  private final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestHAServiceHDFSImpl";
  private TajoTestingCluster util;
  private FileTablespace sm;
  private CatalogService catalog;
  private Path testDir;
  private TableDesc employee;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster(true);

    util.startMaster();
    catalog = util.getCatalogService();

    sm = TablespaceManager.getLocalFs();

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);

    Schema schema = SchemaBuilder.builder()
      .add("managerid", TajoDataTypes.Type.INT4)
      .add("empid", TajoDataTypes.Type.INT4)
      .add("deptname", TajoDataTypes.Type.TEXT)
      .build();

    TableMeta employeeMeta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration());

    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs())
      .getAppender(employeeMeta, schema, employeePath);
    appender.enableStats();
    appender.init();
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
    TajoMaster backupMaster = null;

    try {
      TajoMaster primaryMaster = util.getMaster();
      assertNotNull(primaryMaster);

      TajoConf conf = getBackupMasterConfiguration();
      backupMaster = new TajoMaster();
      backupMaster.init(conf);
      backupMaster.start();
      Assert.assertNotNull(backupMaster);

      ServiceTracker tracker = ServiceTrackerFactory.get(util.getConfiguration());
      assertNotEquals(primaryMaster.getMasterName(), backupMaster.getMasterName());

      FileSystem fs = sm.getFileSystem();
      Path haPath = TajoConf.getSystemHADir(util.getConfiguration());
      assertTrue(fs.exists(haPath));

      Path activePath = new Path(haPath, TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);
      assertTrue(fs.exists(activePath));

      Path backupPath = new Path(haPath, TajoConstants.SYSTEM_HA_BACKUP_DIR_NAME);
      assertTrue(fs.exists(backupPath));

      assertEquals(2, fs.listStatus(activePath).length);
      assertEquals(1, fs.listStatus(backupPath).length);

      assertTrue(fs.exists(new Path(activePath, HAConstants.ACTIVE_LOCK_FILE)));
      assertTrue(fs.exists(new Path(activePath, primaryMaster.getMasterName().replaceAll(":", "_"))));
      assertTrue(fs.exists(new Path(backupPath, backupMaster.getMasterName().replaceAll(":", "_"))));

      createDatabaseAndTable(tracker);
      existDataBaseAndTable(tracker);

      primaryMaster.stop();

      existDataBaseAndTable(tracker);

      assertTrue(fs.exists(new Path(activePath, HAConstants.ACTIVE_LOCK_FILE)));
      assertTrue(fs.exists(new Path(activePath, backupMaster.getMasterName().replaceAll(":", "_"))));

      assertEquals(2, fs.listStatus(activePath).length);
      assertEquals(0, fs.listStatus(backupPath).length);

      assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, "employee"));
    } finally {
      if (backupMaster != null) {
        backupMaster.close();
      }
    }

  }

  private TajoConf getBackupMasterConfiguration() {
    TajoConf conf = new TajoConf(util.getConfiguration());

    conf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS, "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS, "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.CATALOG_ADDRESS, "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.TAJO_MASTER_INFO_ADDRESS, "localhost:" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.REST_SERVICE_ADDRESS, "localhost:" + NetUtils.getFreeSocketPort());

    conf.setBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE, true);
    conf.setIntVar(TajoConf.ConfVars.TAJO_MASTER_HA_MONITOR_INTERVAL, 1000);

    return conf;
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

  private void existDataBaseAndTable(ServiceTracker tracker) throws Exception {
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