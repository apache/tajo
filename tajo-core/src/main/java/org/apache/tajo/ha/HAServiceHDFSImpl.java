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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

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

  private boolean isActiveStatus = false;

  //thread which runs periodically to see the last time since a heartbeat is received.
  private Thread checkerThread;
  private volatile boolean stopped = false;

  private int monitorInterval;

  private String currentActiveMaster;

  public HAServiceHDFSImpl(MasterContext context) throws IOException {
    this.context = context;
    this.conf = context.getConf();
    initSystemDirectory();

    InetSocketAddress socketAddress = context.getTajoMasterService().getBindAddress();
    this.masterName = socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort();

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
      currentActiveMaster = masterName;
      LOG.info(String.format("This is added to active master (%s)", masterName));
    } else {
      // Phase 2: If there is active master information, we need to check its status.
      Path activePath = files[0].getPath();
      currentActiveMaster = activePath.getName().replaceAll("_", ":");

      // Phase 3: If current active master is dead, this master should be active master.
      if (!HAServiceUtil.isMasterAlive(currentActiveMaster, conf)) {
        fs.delete(activePath, true);
        createMasterFile(true);
        currentActiveMaster = masterName;
        LOG.info(String.format("This is added to active master (%s)", masterName));
      } else {
        // Phase 4: If current active master is alive, this master need to be backup master.
        createMasterFile(false);
        LOG.info(String.format("This is added to backup masters (%s)", masterName));
      }
    }
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
    InetSocketAddress address = getHostAddress(HAConstants.MASTER_CLIENT_RPC_ADDRESS);
    sb.append(address.getAddress().getHostAddress()).append(":").append(address.getPort()).append("_");

    address = getHostAddress(HAConstants.RESOURCE_TRACKER_RPC_ADDRESS);
    sb.append(address.getAddress().getHostAddress()).append(":").append(address.getPort()).append("_");

    address = getHostAddress(HAConstants.CATALOG_ADDRESS);
    sb.append(address.getAddress().getHostAddress()).append(":").append(address.getPort()).append("_");

    address = getHostAddress(HAConstants.MASTER_INFO_ADDRESS);
    sb.append(address.getAddress().getHostAddress()).append(":").append(address.getPort());

    FSDataOutputStream out = fs.create(path);

    try {
      out.writeUTF(sb.toString());
      out.hflush();
      out.close();
    } catch (FileAlreadyExistsException e) {
      createMasterFile(false);
    }

    if (isActive) {
      isActiveStatus = true;
    } else {
      isActiveStatus = false;
    }

    startPingChecker();
  }


  private InetSocketAddress getHostAddress(int type) {
    InetSocketAddress address = null;

    switch (type) {
      case HAConstants.MASTER_UMBILICAL_RPC_ADDRESS:
        address = context.getConf().getSocketAddrVar(TajoConf.ConfVars
          .TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
        break;
      case HAConstants.MASTER_CLIENT_RPC_ADDRESS:
        address = context.getConf().getSocketAddrVar(TajoConf.ConfVars
          .TAJO_MASTER_CLIENT_RPC_ADDRESS);
        break;
      case HAConstants.RESOURCE_TRACKER_RPC_ADDRESS:
        address = context.getConf().getSocketAddrVar(TajoConf.ConfVars
          .RESOURCE_TRACKER_RPC_ADDRESS);
        break;
      case HAConstants.CATALOG_ADDRESS:
        address = context.getConf().getSocketAddrVar(TajoConf.ConfVars
          .CATALOG_ADDRESS);
        break;
      case HAConstants.MASTER_INFO_ADDRESS:
        address = context.getConf().getSocketAddrVar(TajoConf.ConfVars
        .TAJO_MASTER_INFO_ADDRESS);
      default:
        break;
    }

    return NetUtils.createSocketAddr(masterName.split(":")[0] + ":" + address.getPort());
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
  public boolean isActiveStatus() {
    return isActiveStatus;
  }

  @Override
  public List<TajoMasterInfo> getMasters() throws IOException {
    List<TajoMasterInfo> list = TUtil.newList();
    Path path = null;

    FileStatus[] files = fs.listStatus(activePath);
    if (files.length == 1) {
      path = files[0].getPath();
      list.add(createTajoMasterInfo(path, true));
    }

    files = fs.listStatus(backupPath);
    for (FileStatus status : files) {
      path = status.getPath();
      list.add(createTajoMasterInfo(path, false));
    }

    return list;
  }

  private TajoMasterInfo createTajoMasterInfo(Path path, boolean isActive) throws IOException {
    String masterAddress = path.getName().replaceAll("_", ":");
    boolean isAlive = HAServiceUtil.isMasterAlive(masterAddress, conf);

    FSDataInputStream stream = fs.open(path);
    String data = stream.readUTF();

    stream.close();

    String[] addresses = data.split("_");
    TajoMasterInfo info = new TajoMasterInfo();

    info.setTajoMasterAddress(NetUtils.createSocketAddr(masterAddress));
    info.setTajoClientAddress(NetUtils.createSocketAddr(addresses[0]));
    info.setWorkerResourceTrackerAddr(NetUtils.createSocketAddr(addresses[1]));
    info.setCatalogAddress(NetUtils.createSocketAddr(addresses[2]));
    info.setWebServerAddress(NetUtils.createSocketAddr(addresses[3]));

    info.setAvailable(isAlive);
    info.setActive(isActive);

    return info;
  }

  private class PingChecker implements Runnable {
    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        synchronized (HAServiceHDFSImpl.this) {
          try {
            if (!currentActiveMaster.equals(masterName)) {
              boolean isAlive = HAServiceUtil.isMasterAlive(currentActiveMaster, conf);
              if (LOG.isDebugEnabled()) {
                LOG.debug("currentActiveMaster:" + currentActiveMaster + ", thisMasterName:" + masterName
                  + ", isAlive:" + isAlive);
              }

              // If active master is dead, this master should be active master instead of
              // previous active master.
              if (!isAlive) {
                FileStatus[] files = fs.listStatus(activePath);
                if (files.length == 0 || (files.length ==  1
                  && currentActiveMaster.equals(files[0].getPath().getName().replaceAll("_", ":")))) {
                  delete();
                  register();
                }
              }
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
