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

package org.apache.tajo.master.ha;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ha.HAServiceUtil;
import org.apache.tajo.master.TajoMaster;
import org.junit.Test;

import java.net.InetAddress;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestHAServiceHDFSImpl  {
  private static Log LOG = LogFactory.getLog(TestHAServiceHDFSImpl.class);

  private TajoTestingCluster cluster;
  private TajoMaster backupMaster1, backupMaster2;

  private TajoConf conf;
  private TajoClient client;
  private Path testDir;

  private Path haPath, activePath, backupPath;

  private static String masterAddress;

  @Test
  public final void testTwoBackupMasters() throws Exception {
    cluster = new TajoTestingCluster(true);

    cluster.startMiniCluster(1);
    conf = cluster.getConfiguration();
    client = new TajoClientImpl(conf);

    try {
      FileSystem fs = cluster.getDefaultFileSystem();

      masterAddress = HAServiceUtil.getMasterUmbilicalName(conf).split(":")[0];
      startBackupMasters();
      verifyMasterAddress();
      verifySystemDirectories(fs);

      Path backupMasterFile1 = new Path(backupPath, backupMaster1.getMasterName()
        .replaceAll(":", "_"));
      assertTrue(fs.exists(backupMasterFile1));

      Path backupMasterFile2 = new Path(backupPath, backupMaster2.getMasterName()
        .replaceAll(":", "_"));
      assertTrue(fs.exists(backupMasterFile2));

      assertTrue(cluster.getMaster().isActiveMaster());
      assertFalse(backupMaster1.isActiveMaster());
      assertFalse(backupMaster2.isActiveMaster());
    } finally {
      IOUtils.cleanup(LOG, client, backupMaster1, backupMaster2);
      cluster.shutdownMiniCluster();
    }
  }

  private void setConfiguration() {
    conf = cluster.getConfiguration();
    conf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS,
      masterAddress + ":" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS,
      masterAddress + ":" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS,
      masterAddress + ":" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.CATALOG_ADDRESS,
      masterAddress + ":" + NetUtils.getFreeSocketPort());
    conf.setVar(TajoConf.ConfVars.TAJO_MASTER_INFO_ADDRESS,
      masterAddress + ":" + NetUtils.getFreeSocketPort());
    conf.setBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE, true);
  }

  private void startBackupMasters() throws Exception {
    setConfiguration();
    backupMaster1 = new TajoMaster();
    backupMaster1.init(conf);
    backupMaster1.start();

    setConfiguration();
    backupMaster2 = new TajoMaster();
    backupMaster2.init(conf);
    backupMaster2.start();
  }

  private void verifyMasterAddress() {
    assertNotEquals(cluster.getMaster().getMasterName(),
      backupMaster1.getMasterName());
    assertNotEquals(cluster.getMaster().getMasterName(),
      backupMaster2.getMasterName());
    assertNotEquals(backupMaster1.getMasterName(),
      backupMaster2.getMasterName());
  }

  private void verifySystemDirectories(FileSystem fs) throws Exception {
    haPath = TajoConf.getSystemHADir(cluster.getConfiguration());
    assertTrue(fs.exists(haPath));

    activePath = new Path(haPath, TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);
    assertTrue(fs.exists(activePath));

    backupPath = new Path(haPath, TajoConstants.SYSTEM_HA_BACKUP_DIR_NAME);
    assertTrue(fs.exists(backupPath));

    assertEquals(1, fs.listStatus(activePath).length);
    assertEquals(2, fs.listStatus(backupPath).length);
  }
}
