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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.service.HAServiceTracker;
import org.apache.tajo.service.ServiceTrackerException;
import org.apache.tajo.service.TajoMasterInfo;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * This implements HAService utilizing HDFS cluster. This saves master status to HDFS cluster.
 *
 */
@SuppressWarnings("unused")
public class HdfsServiceTracker extends HAServiceTracker {
  private static Log LOG = LogFactory.getLog(HdfsServiceTracker.class);

  private TajoConf conf;

  private FileSystem fs;

  private String masterName;
  private Path rootPath;
  private Path haPath;
  private Path activePath;
  private Path backupPath;

  private boolean isActiveMaster = false;

  //thread which runs periodically to see the last time since a heartbeat is received.
  private Thread checkerThread;
  private volatile boolean stopped = false;

  private int monitorInterval;

  private String currentActiveMaster;

  public HdfsServiceTracker(TajoConf conf) throws IOException {
    this.conf = conf;
    initSystemDirectory();

    InetSocketAddress socketAddress = conf.getSocketAddrVar(ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
    this.masterName = socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort();
    this.monitorInterval = conf.getIntVar(ConfVars.TAJO_MASTER_HA_MONITOR_INTERVAL);
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

  /**
   * It will creates the following form string. It includes
   *
   * <pre>
   * {CLIENT_RPC_HOST:PORT}_{RESOURCE_TRACKER_HOST:PORT}_{CATALOG_HOST:PORT}_{MASTER_WEB_HOST:PORT}
   * </pre>
   *
   * @throws IOException
   */
  @Override
  public void register() throws IOException {
    // Check lock file
    boolean lockResult = createLockFile();

    String fileName = masterName.replaceAll(":", "_");
    Path activeFile = new Path(activePath, fileName);
    Path backupFile = new Path(backupPath, fileName);

    // Set TajoMasterInfo object which has several rpc server addresses.
    StringBuilder sb = new StringBuilder();
    InetSocketAddress address = getHostAddress(HAConstants.MASTER_UMBILICAL_RPC_ADDRESS);
    sb.append(address.getAddress().getHostAddress()).append(":").append(address.getPort()).append("_");

    address = getHostAddress(HAConstants.MASTER_CLIENT_RPC_ADDRESS);
    sb.append(address.getAddress().getHostAddress()).append(":").append(address.getPort()).append("_");

    address = getHostAddress(HAConstants.RESOURCE_TRACKER_RPC_ADDRESS);
    sb.append(address.getAddress().getHostAddress()).append(":").append(address.getPort()).append("_");

    address = getHostAddress(HAConstants.CATALOG_ADDRESS);
    sb.append(address.getAddress().getHostAddress()).append(":").append(address.getPort()).append("_");

    address = getHostAddress(HAConstants.MASTER_INFO_ADDRESS);
    sb.append(address.getAddress().getHostAddress()).append(":").append(address.getPort());

    // Phase 1: If there is not another active master, this try to become active master.
    if (lockResult) {
      fs.delete(backupFile, false);
      createMasterFile(activeFile, sb);
      currentActiveMaster = masterName;
      writeSystemConf();
      LOG.info(String.format("This is added to active master (%s)", masterName));
    } else {
      // Phase 2: If there is active master information, we need to check its status.
      FileStatus[] files = fs.listStatus(activePath);
      Path existingActiveFile = null;
      if (files.length > 2) {
        throw new ServiceTrackerException("Three or more than active master entries.");
      }
      for(FileStatus eachFile : files) {
        if (!eachFile.getPath().getName().equals(HAConstants.ACTIVE_LOCK_FILE)) {
          existingActiveFile = eachFile.getPath();
        }
      }
      currentActiveMaster = existingActiveFile.getName().replaceAll("_", ":");

      // Phase 3: If current active master is dead, this master should be active master.
      if (!checkConnection(currentActiveMaster)) {
        fs.delete(existingActiveFile, false);
        fs.delete(backupFile, false);
        createMasterFile(activeFile, sb);
        currentActiveMaster = masterName;
        LOG.info(String.format("This is added to active master (%s)", masterName));
      } else {
        // Phase 4: If current active master is alive, this master need to be backup master.
        if (masterName.equals(currentActiveMaster)) {
          LOG.info(String.format("This has already been added to active master (%s)", masterName));
        } else {
          if (fs.exists(backupFile)) {
            LOG.info(String.format("This has already been added to backup masters (%s)", masterName));
          } else {
            createMasterFile(backupFile, sb);
            LOG.info(String.format("This is added to backup master (%s)", masterName));
          }
        }
      }
    }

    if(!isActiveMaster()) {
      startPingChecker();
    }
  }

  /**
   * Storing the system configs
   *
   * @throws IOException
   */
  private void writeSystemConf() throws IOException {
    Path systemConfPath = TajoConf.getSystemConfPath(conf);

    FSDataOutputStream out = FileSystem.create(fs, systemConfPath,
      new FsPermission(TajoMaster.SYSTEM_CONF_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
    fs.setReplication(systemConfPath, (short) conf.getIntVar(ConfVars.SYSTEM_CONF_REPLICA_COUNT));
  }

  private boolean createLockFile() throws IOException {
    boolean result = false;
    FSDataOutputStream lockOutput = null;

    Path lockFile = new Path(activePath, HAConstants.ACTIVE_LOCK_FILE);
    try {
      lockOutput = fs.create(lockFile, false);
      lockOutput.close();
      fs.deleteOnExit(lockFile);
      result = true;
    } catch (FileAlreadyExistsException e) {
      LOG.info(String.format("Lock file already exists at (%s)", lockFile.toString()));
      result = false;
    } catch (Exception e) {
      throw new IOException("Lock file creation is failed - " + e.getMessage());
    } finally {
      FileUtil.cleanup(LOG, lockOutput);
    }

    return result;
  }

  private void createMasterFile(Path path, StringBuilder sb) throws IOException {
    FSDataOutputStream out = null;
    try {
      out = fs.create(path, false);

      out.writeUTF(sb.toString());
      out.close();

      fs.deleteOnExit(path);
    } catch (Exception e) {
      throw new IOException("File creation is failed - " + e.getMessage());
    } finally {
      FileUtil.cleanup(LOG, out);
    }
  }

  private InetSocketAddress getHostAddress(int type) {
    InetSocketAddress address = null;

    switch (type) {
      case HAConstants.MASTER_UMBILICAL_RPC_ADDRESS:
        address = conf.getSocketAddrVar(ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
        break;
      case HAConstants.MASTER_CLIENT_RPC_ADDRESS:
        address = conf.getSocketAddrVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS);
        break;
      case HAConstants.RESOURCE_TRACKER_RPC_ADDRESS:
        address = conf.getSocketAddrVar(ConfVars.RESOURCE_TRACKER_RPC_ADDRESS);
        break;
      case HAConstants.CATALOG_ADDRESS:
        address = conf.getSocketAddrVar(ConfVars.CATALOG_ADDRESS);
        break;
      case HAConstants.MASTER_INFO_ADDRESS:
        address = conf.getSocketAddrVar(ConfVars.TAJO_MASTER_INFO_ADDRESS);
        break;
      default:
        break;
    }

    if (address != null) {
      return NetUtils.createSocketAddr(masterName.split(":")[0] + ":" + address.getPort());
    } else {
      return null;
    }
  }

  @Override
  public synchronized void delete() throws IOException {
    stopped = true;

    if (ShutdownHookManager.get().isShutdownInProgress()) return;

    String fileName = masterName.replaceAll(":", "_");

    fs.delete(new Path(activePath, fileName), false);
    fs.delete(new Path(activePath, HAConstants.ACTIVE_LOCK_FILE), false);
    fs.delete(new Path(backupPath, fileName), false);
  }

  @Override
  public boolean isActiveMaster() {
    if (currentActiveMaster.equals(masterName)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public List<TajoMasterInfo> getMasters() throws IOException {
    List<TajoMasterInfo> list = TUtil.newList();

    FileStatus[] files = fs.listStatus(activePath);
    for(FileStatus status : files) {
      if (!status.getPath().getName().equals(HAConstants.ACTIVE_LOCK_FILE)) {
        list.add(getTajoMasterInfo(status.getPath(), true));
      }
    }

    files = fs.listStatus(backupPath);
    for (FileStatus status : files) {
      list.add(getTajoMasterInfo(status.getPath(), false));
    }

    return list;
  }

  private TajoMasterInfo getTajoMasterInfo(Path path, boolean isActive) throws IOException {
    String masterAddress = path.getName().replaceAll("_", ":");
    boolean isAlive = checkConnection(masterAddress);

    FSDataInputStream stream = fs.open(path);
    String data = stream.readUTF();
    stream.close();

    String[] addresses = data.split("_");
    TajoMasterInfo info = new TajoMasterInfo();

    info.setTajoMasterAddress(NetUtils.createSocketAddr(addresses[0]));
    info.setTajoClientAddress(NetUtils.createSocketAddr(addresses[1]));
    info.setWorkerResourceTrackerAddr(NetUtils.createSocketAddr(addresses[2]));
    info.setCatalogAddress(NetUtils.createSocketAddr(addresses[3]));
    info.setWebServerAddress(NetUtils.createSocketAddr(addresses[4]));

    info.setAvailable(isAlive);
    info.setActive(isActive);

    return info;
  }

  private class PingChecker implements Runnable {
    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(monitorInterval);
        } catch (InterruptedException e) {
          LOG.info("PingChecker interrupted. - masterName:" + masterName);
          break;
        }

        synchronized (HdfsServiceTracker.this) {
          try {
            if (!currentActiveMaster.equals(masterName)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("currentActiveMaster:" + currentActiveMaster + ", thisMasterName:" + masterName);
              }

              // If active master is dead, this master should be active master instead of
              // previous active master.
              if (!stopped && !checkConnection(currentActiveMaster)) {
                Path activeFile = new Path(activePath, currentActiveMaster.replaceAll(":", "_"));
                fs.delete(activeFile, false);

                Path lockFile = new Path(activePath, HAConstants.ACTIVE_LOCK_FILE);
                fs.delete(lockFile, false);
                register();
              }
            }
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          }
        }
      }
    }
  }

  private final static int MASTER_UMBILICAL_RPC_ADDRESS = 0;
  private final static int MASTER_CLIENT_RPC_ADDRESS = 1;
  private final static int RESOURCE_TRACKER_RPC_ADDRESS = 2;
  private final static int CATALOG_ADDRESS = 3;
  private final static int MASTER_HTTP_INFO = 4;

  private volatile InetSocketAddress umbilicalRpcAddr;
  private volatile InetSocketAddress clientRpcAddr;
  private volatile InetSocketAddress resourceTrackerRpcAddr;
  private volatile InetSocketAddress catalogAddr;
  private volatile InetSocketAddress masterHttpInfoAddr;

  @Override
  public InetSocketAddress getUmbilicalAddress() {
    if (!checkConnection(umbilicalRpcAddr)) {
      umbilicalRpcAddr = NetUtils.createSocketAddr(getAddressElements().get(MASTER_UMBILICAL_RPC_ADDRESS));
    }

    return umbilicalRpcAddr;
  }

  @Override
  public InetSocketAddress getClientServiceAddress() {
    if (!checkConnection(clientRpcAddr)) {
      clientRpcAddr = NetUtils.createSocketAddr(getAddressElements().get(MASTER_CLIENT_RPC_ADDRESS));
    }

    return clientRpcAddr;
  }

  @Override
  public InetSocketAddress getResourceTrackerAddress() {
    if (!checkConnection(resourceTrackerRpcAddr)) {
      resourceTrackerRpcAddr = NetUtils.createSocketAddr(getAddressElements().get(RESOURCE_TRACKER_RPC_ADDRESS));
    }

    return resourceTrackerRpcAddr;
  }

  @Override
  public InetSocketAddress getCatalogAddress() {
    if (!checkConnection(catalogAddr)) {
      catalogAddr = NetUtils.createSocketAddr(getAddressElements().get(CATALOG_ADDRESS));
    }

    return catalogAddr;
  }

  @Override
  public InetSocketAddress getMasterHttpInfo() throws ServiceTrackerException {
    if (!checkConnection(masterHttpInfoAddr)) {
      masterHttpInfoAddr = NetUtils.createSocketAddr(getAddressElements().get(MASTER_HTTP_INFO));
    }

    return masterHttpInfoAddr;
  }

  /**
   * Reads a text file stored in HDFS file, and then return all service addresses read from a HDFS file.   *
   *
   * @return all service addresses
   * @throws ServiceTrackerException
   */
  private synchronized List<String> getAddressElements() throws ServiceTrackerException {

    try {
      FileSystem fs = getFileSystem(conf);
      Path activeMasterBaseDir = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);

      if (!fs.exists(activeMasterBaseDir)) {
        throw new ServiceTrackerException("No such active master base path: " + activeMasterBaseDir);
      }
      if (!fs.isDirectory(activeMasterBaseDir)) {
        throw new ServiceTrackerException("Active master base path must be a directory.");
      }

      /* wait for active master from HDFS */
      int pause = conf.getIntVar(ConfVars.TAJO_MASTER_HA_CLIENT_RETRY_PAUSE_TIME);
      int maxRetry = conf.getIntVar(ConfVars.TAJO_MASTER_HA_CLIENT_RETRY_MAX_NUM);
      int retry = 0;


      Path activeMasterEntry = null;
      FileStatus[] files = null;

      loop:while (retry < maxRetry) {
        files = fs.listStatus(activeMasterBaseDir);
        for (FileStatus eachFile : files) {
          //check if active file is written
          if (!eachFile.getPath().getName().equals(HAConstants.ACTIVE_LOCK_FILE) && eachFile.getLen() > 0) {
            activeMasterEntry = eachFile.getPath();
            break loop;
          }
        }

        try {
          this.wait(pause);
        } catch (InterruptedException e) {
          throw new ServiceTrackerException(e);
        }
      }

      if (files == null || activeMasterEntry == null) {
        throw new ServiceTrackerException("Active master entry cannot be found in: " + activeMasterBaseDir);
      }

      if (files.length < 1) {
        LOG.error("Exceeded the maximum retry (" + maxRetry + ") to read TajoMaster address from HDFS");
        throw new ServiceTrackerException("No active master entry");
      } else if (files.length > 2) {
        throw new ServiceTrackerException("Three or more than active master entries.");
      }

      if (!fs.isFile(activeMasterEntry)) {
        throw new ServiceTrackerException("Active master entry must be a file, but it is a directory.");
      }

      List<String> addressElements = TUtil.newList();

      FSDataInputStream stream = fs.open(activeMasterEntry);
      String data = stream.readUTF();
      stream.close();
      addressElements.addAll(TUtil.newList(data.split("_"))); // Add remains entries to elements

      // ensure the number of entries
      Preconditions.checkState(addressElements.size() == 5, "Fewer service addresses than necessary.");

      return addressElements;

    } catch (Throwable t) {
      throw new ServiceTrackerException(t);
    }
  }

  @Override
  public int getState(String masterName, TajoConf conf) throws ServiceTrackerException {
    String targetMaster = masterName.replaceAll(":", "_");
    int retValue = -1;

    try {
      FileSystem fs = getFileSystem(conf);
      Path activePath = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);
      Path backupPath = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_BACKUP_DIR_NAME);

      Path temPath = null;

      // Check backup masters
      FileStatus[] files = fs.listStatus(backupPath);
      for (FileStatus status : files) {
        temPath = status.getPath();
        if (temPath.getName().equals(targetMaster)) {
          return 0;
        }
      }

      // Check active master
      files = fs.listStatus(activePath);
      if (files.length == 1) {
        temPath = files[0].getPath();
        if (temPath.getName().equals(targetMaster)) {
          return 1;
        }
      }
      retValue = -2;
    } catch (Exception e) {
      throw new ServiceTrackerException("Cannot get HA state - ERROR:" + e.getMessage());
    }
    return retValue;
  }

  @Override
  public int formatHA(TajoConf conf) throws ServiceTrackerException{
    int retValue = -1;
    try {
      FileSystem fs = getFileSystem(conf);
      Path activePath = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);
      Path backupPath = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_BACKUP_DIR_NAME);
      Path temPath = null;

      int aliveMasterCount = 0;

      // Check backup masters
      FileStatus[] files = fs.listStatus(backupPath);
      for (FileStatus eachFile : files) {
        if (checkConnection(eachFile.getPath().getName(), "_")) {
          aliveMasterCount++;
        }
      }

      // Check active master
      files = fs.listStatus(activePath);
      for (FileStatus eachFile : files) {
        if (!eachFile.getPath().getName().equals(HAConstants.ACTIVE_LOCK_FILE) &&
            checkConnection(eachFile.getPath().getName(), "_")) {
          aliveMasterCount++;
        }
      }

      // If there is any alive master, users can't format storage.
      if (aliveMasterCount > 0) {
        return 0;
      }

      // delete ha path.
      fs.delete(TajoConf.getSystemHADir(conf), true);
      retValue = 1;
    } catch (Exception e) {
      throw new ServiceTrackerException("Cannot format HA directories - ERROR:" + e.getMessage());
    }
    return retValue;
  }

  @Override
  public List<String> getMasters(TajoConf conf) throws ServiceTrackerException {
    List<String> list = new ArrayList<String>();

    try {
      FileSystem fs = getFileSystem(conf);
      Path activePath = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);
      Path backupPath = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_BACKUP_DIR_NAME);
      Path temPath = null;

      // Check backup masters
      FileStatus[] files = fs.listStatus(backupPath);
      for (FileStatus status : files) {
        temPath = status.getPath();
        list.add(temPath.getName().replaceAll("_", ":"));
      }

      // Check active master
      files = fs.listStatus(activePath);
      if (files.length == 1) {
        temPath = files[0].getPath();
        list.add(temPath.getName().replaceAll("_", ":"));
      }

    } catch (Exception e) {
      throw new ServiceTrackerException("Cannot get master lists - ERROR:" + e.getMessage());
    }
    return list;
  }

  private static FileSystem getFileSystem(TajoConf conf) throws IOException {
    Path rootPath = TajoConf.getTajoRootDir(conf);
    return rootPath.getFileSystem(conf);
  }
}