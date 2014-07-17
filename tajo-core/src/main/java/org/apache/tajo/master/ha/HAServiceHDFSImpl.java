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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.TajoMaster.MasterContext;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * This implements HAService utilizing HDFS cluster. This saves master status to HDFS cluster.
 *
 */
public class HAServiceHDFSImpl implements HAService {
  private static Log LOG = LogFactory.getLog(HAServiceHDFSImpl.class);

  private MasterContext context;
  private TajoConf conf;

  private FileSystem fs;

  private String masterName;
  private Path rootPath;
  private Path haPath;
  private Path activePath;
  private Path backupPath;

  // how to create sockets
  private SocketFactory socketFactory;
  // connected socket
  private Socket socket = null;
  private int connectionTimeout;

  private boolean isActiveStatus = false;

  //thread which runs periodically to see the last time since a heartbeat is received.
  private Thread checkerThread;
  private volatile boolean stopped = false;

  private int monitorInterval;

  public HAServiceHDFSImpl(MasterContext context, String masterName) throws IOException {
    this.context = context;

    this.conf = context.getConf();
    initSystemDirectory();

    socketFactory = NetUtils.getDefaultSocketFactory(conf);

    connectionTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);

    this.masterName = masterName;

    monitorInterval = conf.getIntVar(TajoConf.ConfVars.TAJO_MASTER_HA_MONITOR_INTERVAL);
  }

  private void initSystemDirectory() throws IOException {
    // Get Tajo root dir
    this.rootPath = TajoConf.getTajoRootDir(conf);

    // Check Tajo root dir
    this.fs = rootPath.getFileSystem(conf);

    // Check and create Tajo system HA dir
    haPath = TajoConf.getSystemHADir(conf);
    if (!fs.exists(haPath)) {
      fs.mkdirs(haPath, new FsPermission(TajoMaster.SYSTEM_RESOURCE_DIR_PERMISSION));
      LOG.info("System HA dir '" + haPath + "' is created");
    }

    activePath = new Path(haPath, TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);
    if (!fs.exists(activePath)) {
      fs.mkdirs(activePath, new FsPermission(TajoMaster.SYSTEM_RESOURCE_DIR_PERMISSION));
      LOG.info("System HA Active dir '" + activePath + "' is created");
    }

    backupPath = new Path(haPath, TajoConstants.SYSTEM_HA_BACKUP_DIR_NAME);
    if (!fs.exists(backupPath)) {
      fs.mkdirs(backupPath, new FsPermission(TajoMaster.SYSTEM_RESOURCE_DIR_PERMISSION));
      LOG.info("System HA Backup dir '" + backupPath + "' is created");
    }
  }

  private void startPingChecker() {
    if (checkerThread == null) {
      checkerThread = new Thread(new PingChecker());
      checkerThread.setName("Ping Checker");
      checkerThread.start();
    }
  }

  @Override
  public void register() throws IOException {
    FileStatus[] files = fs.listStatus(activePath);

    // Phase 1: If there is not another active master, this try to become active master.
    if (files.length == 0) {
      createMasterFile(true);
      LOG.info(String.format("This is added to active master (%s)", masterName));
    } else {
      // Phase 2: If there is active master information, we need to check its status.
      Path activePath = files[0].getPath();
      String currentActiveMaster = activePath.getName().replaceAll("_", ":");

      // Phase 3: If current active master is dead, this master should be active master.
      if (!isMasterAlive(currentActiveMaster)) {
        fs.delete(activePath, true);
        createMasterFile(true);
        LOG.info(String.format("This is added to active master (%s)", masterName));
      } else {
        // Phase 4: If current active master is alive, this master need to be backup master.
        createMasterFile(false);
        LOG.info(String.format("This is added to backup masters (%s)", masterName));
      }
    }
  }

  private boolean isMasterAlive(String masterName) {
    boolean isAlive = true;

    try {
      InetSocketAddress server = NetUtils.createSocketAddr(masterName);
      socket = socketFactory.createSocket();
      NetUtils.connect(socket, server, connectionTimeout);
    } catch (Exception e) {
      isAlive = false;
    }
    return isAlive;
  }

  private void createMasterFile(boolean isActive) throws IOException {
    String fileName = masterName.replaceAll(":", "_");
    Path path = null;

    if (isActive) {
      path = new Path(activePath, fileName);
    } else {
      path = new Path(backupPath, fileName);
    }

    StringBuilder sb = new StringBuilder();
    sb.append(context.getConf().get(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS.varname));
    sb.append("_");
    sb.append(context.getConf().get(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS.varname));
    sb.append("_");
    sb.append(context.getConf().get(TajoConf.ConfVars.CATALOG_ADDRESS.varname));

    FSDataOutputStream out = fs.create(path);
    out.writeUTF(sb.toString());
    out.close();

    if (isActive) {
      isActiveStatus = true;
    } else {
      isActiveStatus = false;
    }

    FSDataInputStream input = fs.open(path);
    String data = input.readUTF();
    input.close();

    startPingChecker();
  }

  @Override
  public void delete() throws IOException {
    String fileName = masterName.replaceAll(":", "_");

    Path activeFile = new Path(activePath, fileName);
    if (fs.exists(activeFile)) {
      fs.delete(activeFile, true);
    }

    Path backupFile = new Path(backupPath, fileName);
    if (fs.exists(backupFile)) {
      fs.delete(backupFile, true);
    }

    if (isActiveStatus) {
      isActiveStatus = false;
    }
    stopped = true;
  }

  @Override
  public void transitActiveStatus() throws IOException {
    // TODO: This will be implemented for HA admin tools.
  }

  @Override
  public boolean isActiveStatus() {
    return isActiveStatus;
  }

  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        synchronized (HAServiceHDFSImpl.this) {
          try {
            FileStatus[] files = fs.listStatus(activePath);

            if (files.length == 1) {
              Path activePath = files[0].getPath();

              String currentActiveMaster = activePath.getName().replaceAll("_", ":");
              boolean isAlive = isMasterAlive(currentActiveMaster);
              if (LOG.isDebugEnabled()) {
                LOG.debug("master:" + currentActiveMaster + ", isAlive:" + isAlive);
              }

              // If active master is dead, this master should be active master instead of
              // previous active master.
              if (!isAlive) {
                delete();
                register();
              }
            } else {
              register();
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        try {
          Thread.sleep(monitorInterval);
        } catch (InterruptedException e) {
          LOG.info("PingChecker interrupted. - masterName:" + masterName);
          break;
        }
      }
    }
  }
}
