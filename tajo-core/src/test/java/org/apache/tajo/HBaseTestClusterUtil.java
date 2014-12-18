/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.tajo.util.Bytes;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_ENABLE_KEY;

public class HBaseTestClusterUtil {
  private static final Log LOG = LogFactory.getLog(HBaseTestClusterUtil.class);
  private Configuration conf;
  private MiniHBaseCluster hbaseCluster;
  private MiniZooKeeperCluster zkCluster;
  private File testBaseDir;
  public HBaseTestClusterUtil(Configuration conf, File testBaseDir) {
    this.conf = conf;
    this.testBaseDir = testBaseDir;
  }
  /**
   * Returns the path to the default root dir the minicluster uses.
   * Note: this does not cause the root dir to be created.
   * @return Fully qualified path for the default hbase root dir
   * @throws java.io.IOException
   */
  public Path getDefaultRootDirPath() throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    return new Path(fs.makeQualified(fs.getHomeDirectory()),"hbase");
  }

  /**
   * Creates an hbase rootdir in user home directory.  Also creates hbase
   * version file.  Normally you won't make use of this method.  Root hbasedir
   * is created for you as part of mini cluster startup.  You'd only use this
   * method if you were doing manual operation.
   * @return Fully qualified path to hbase root dir
   * @throws java.io.IOException
   */
  public Path createRootDir() throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    Path hbaseRootdir = getDefaultRootDirPath();
    FSUtils.setRootDir(this.conf, hbaseRootdir);
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    return hbaseRootdir;
  }

  public void stopHBaseCluster() throws IOException {
    if (hbaseCluster != null) {
      LOG.info("MiniHBaseCluster stopped");
      hbaseCluster.shutdown();
      hbaseCluster.waitUntilShutDown();
      hbaseCluster = null;
    }
  }

  public void startHBaseCluster() throws Exception {
    if (zkCluster == null) {
      startMiniZKCluster();
    }
    if (hbaseCluster != null) {
      return;
    }

    System.setProperty("HBASE_ZNODE_FILE", testBaseDir + "/hbase_znode_file");
    if (conf.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, -1) == -1) {
      conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    }
    if (conf.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, -1) == -1) {
      conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 1);
    }
    conf.setBoolean(REPLICATION_ENABLE_KEY, false);
    createRootDir();

    Configuration c = HBaseConfiguration.create(this.conf);
    // randomize hbase info port
    c.setInt(HConstants.MASTER_INFO_PORT, 0);

    hbaseCluster = new MiniHBaseCluster(c, 1);

    // Don't leave here till we've done a successful scan of the hbase:meta
    HTable t = new HTable(c, TableName.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      continue;
    }
    s.close();
    t.close();
    LOG.info("MiniHBaseCluster started");

  }

  /**
   * Start a mini ZK cluster. If the property "test.hbase.zookeeper.property.clientPort" is set
   *  the port mentionned is used as the default port for ZooKeeper.
   */
  public MiniZooKeeperCluster startMiniZKCluster()
      throws Exception {
    File zkDataPath = new File(testBaseDir, "zk");
    if (this.zkCluster != null) {
      throw new IOException("Cluster already running at " + zkDataPath);
    }
    this.zkCluster = new MiniZooKeeperCluster(conf);
    final int defPort = this.conf.getInt("test.hbase.zookeeper.property.clientPort", 0);
    if (defPort > 0){
      // If there is a port in the config file, we use it.
      this.zkCluster.setDefaultClientPort(defPort);
    }
    int clientPort =  this.zkCluster.startup(zkDataPath, 1);
    this.conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));
    LOG.info("MiniZooKeeperCluster started");

    return this.zkCluster;
  }

  public void stopZooKeeperCluster() throws IOException {
    if (zkCluster != null) {
      LOG.info("MiniZooKeeperCluster stopped");
      zkCluster.shutdown();
      zkCluster = null;
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public MiniZooKeeperCluster getMiniZooKeeperCluster() {
    return zkCluster;
  }

  public MiniHBaseCluster getMiniHBaseCluster() {
    return hbaseCluster;
  }

  public HTableDescriptor getTableDescriptor(String tableName) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      return admin.getTableDescriptor(Bytes.toBytes(tableName));
    } finally {
      admin.close();
    }
  }

  public void createTable(HTableDescriptor hTableDesc) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      admin.createTable(hTableDesc);
    } finally {
      admin.close();
    }
  }
}
