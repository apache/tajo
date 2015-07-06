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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestHAServiceHDFSImpl  {
  private TajoTestingCluster cluster;

  private TajoMaster primaryMaster;
  private TajoMaster backupMaster;

  private Path haPath, activePath, backupPath;

  @Test
  public final void testAutoFailOver() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();

    try {
      FileSystem fs = cluster.getDefaultFileSystem();

      TajoConf primaryConf = setConfigForHAMaster();
      primaryMaster = new TajoMaster();
      primaryMaster.init(primaryConf);
      primaryMaster.start();

      TajoConf backupConf = setConfigForHAMaster();
      backupMaster = new TajoMaster();
      backupMaster.init(backupConf);
      backupMaster.start();

      ServiceTracker tracker = ServiceTrackerFactory.get(primaryConf);

      assertNotEquals(primaryMaster.getMasterName(), backupMaster.getMasterName());
      verifySystemDirectories(fs);

      assertEquals(2, fs.listStatus(activePath).length);
      assertEquals(1, fs.listStatus(backupPath).length);

      assertTrue(fs.exists(new Path(activePath, HAConstants.ACTIVE_LOCK_FILE)));
      assertTrue(fs.exists(new Path(activePath, primaryMaster.getMasterName().replaceAll(":", "_"))));
      assertTrue(fs.exists(new Path(backupPath, backupMaster.getMasterName().replaceAll(":", "_"))));

      createDatabaseAndTable(tracker);
      verifyDataBaseAndTable(tracker);

      primaryMaster.stop();

      verifyDataBaseAndTable(tracker);

      assertEquals(2, fs.listStatus(activePath).length);
      assertEquals(0, fs.listStatus(backupPath).length);

      assertTrue(fs.exists(new Path(activePath, HAConstants.ACTIVE_LOCK_FILE)));
      assertTrue(fs.exists(new Path(activePath, backupMaster.getMasterName().replaceAll(":", "_"))));
    } finally {
      backupMaster.stop();
    }
  }

  private TajoConf setConfigForHAMaster() {
    TajoConf conf = new TajoConf(cluster.getConfiguration());

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
    conf.setIntVar(TajoConf.ConfVars.REST_SERVICE_PORT,
        NetUtils.getFreeSocketPort());

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

  private void verifySystemDirectories(FileSystem fs) throws Exception {
    haPath = TajoConf.getSystemHADir(cluster.getConfiguration());
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
      client.existDatabase("default");
      client.existTable("default.ha_test1");
      client.existTable("default.ha_test2");
    } finally {
      IOUtils.cleanup(null, client);
    }
  }
}
