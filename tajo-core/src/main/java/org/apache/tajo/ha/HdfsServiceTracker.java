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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.service.HAServiceTracker;
import org.apache.tajo.service.ServiceTrackerException;
import org.apache.tajo.service.TajoMasterInfo;
import org.apache.tajo.util.TUtil;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
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

  private boolean isActiveStatus = false;

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

    monitorInterval = conf.getIntVar(ConfVars.TAJO_MASTER_HA_MONITOR_INTERVAL);
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

  /**
   * It will creates the following form string. It includes
   *
   * <pre>
   * {CLIENT_RPC_HOST:PORT}_{RESOURCE_TRACKER_HOST:PORT}_{CATALOG_HOST:PORT}_{MASTER_WEB_HOST:PORT}
   * </pre>
   *
   * @param isActive A boolean flag to indicate if it is for master or not.
   * @throws IOException
   */
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
      out.hsync();
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
        synchronized (HdfsServiceTracker.this) {
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
      umbilicalRpcAddr = NetUtils.createSocketAddr(getAddressElements(conf).get(MASTER_UMBILICAL_RPC_ADDRESS));
    }

    return umbilicalRpcAddr;
  }

  @Override
  public InetSocketAddress getClientServiceAddress() {
    if (!checkConnection(clientRpcAddr)) {
      clientRpcAddr = NetUtils.createSocketAddr(getAddressElements(conf).get(MASTER_CLIENT_RPC_ADDRESS));
    }

    return clientRpcAddr;
  }

  @Override
  public InetSocketAddress getResourceTrackerAddress() {
    if (!checkConnection(resourceTrackerRpcAddr)) {
      resourceTrackerRpcAddr = NetUtils.createSocketAddr(getAddressElements(conf).get(RESOURCE_TRACKER_RPC_ADDRESS));
    }

    return resourceTrackerRpcAddr;
  }

  @Override
  public InetSocketAddress getCatalogAddress() {
    if (!checkConnection(catalogAddr)) {
      catalogAddr = NetUtils.createSocketAddr(getAddressElements(conf).get(CATALOG_ADDRESS));
    }

    return catalogAddr;
  }

  @Override
  public InetSocketAddress getMasterHttpInfo() throws ServiceTrackerException {
    if (!checkConnection(masterHttpInfoAddr)) {
      masterHttpInfoAddr = NetUtils.createSocketAddr(getAddressElements(conf).get(MASTER_HTTP_INFO));
    }

    return masterHttpInfoAddr;
  }

  /**
   * Reads a text file stored in HDFS file, and then return all service addresses read from a HDFS file.   *
   *
   * @param conf
   * @return all service addresses
   * @throws ServiceTrackerException
   */
  private static List<String> getAddressElements(TajoConf conf) throws ServiceTrackerException {

    try {
      FileSystem fs = getFileSystem(conf);
      Path activeMasterBaseDir = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);

      if (!fs.exists(activeMasterBaseDir)) {
        throw new ServiceTrackerException("No such active master base path: " + activeMasterBaseDir);
      }
      if (!fs.isDirectory(activeMasterBaseDir)) {
        throw new ServiceTrackerException("Active master base path must be a directory.");
      }

      FileStatus[] files = fs.listStatus(activeMasterBaseDir);

      if (files.length < 1) {
        throw new ServiceTrackerException("No active master entry");
      } else if (files.length > 1) {
        throw new ServiceTrackerException("Two or more than active master entries.");
      }

      // We can ensure that there is only one file due to the above assertion.
      Path activeMasterEntry = files[0].getPath();

      if (!fs.isFile(activeMasterEntry)) {
        throw new ServiceTrackerException("Active master entry must be a file, but it is a directory.");
      }

      List<String> addressElements = TUtil.newList();

      addressElements.add(activeMasterEntry.getName().replaceAll("_", ":")); // Add UMBILICAL_RPC_ADDRESS to elements

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


  public static boolean isMasterAlive(InetSocketAddress masterAddress, TajoConf conf) {
    return isMasterAlive(org.apache.tajo.util.NetUtils.normalizeInetSocketAddress(masterAddress), conf);
  }

  public static boolean isMasterAlive(String masterName, TajoConf conf) {
    boolean isAlive = true;

    try {
      // how to create sockets
      SocketFactory socketFactory = org.apache.hadoop.net.NetUtils.getDefaultSocketFactory(conf);

      int connectionTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
          CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);

      InetSocketAddress server = org.apache.hadoop.net.NetUtils.createSocketAddr(masterName);

      // connected socket
      Socket socket = socketFactory.createSocket();
      org.apache.hadoop.net.NetUtils.connect(socket, server, connectionTimeout);
    } catch (Exception e) {
      isAlive = false;
    }
    return isAlive;
  }

  public static int getState(String masterName, TajoConf conf) {
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
      e.printStackTrace();
    }
    return retValue;
  }

  public static int formatHA(TajoConf conf) {
    int retValue = -1;
    try {
      FileSystem fs = getFileSystem(conf);
      Path activePath = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);
      Path backupPath = new Path(TajoConf.getSystemHADir(conf), TajoConstants.SYSTEM_HA_BACKUP_DIR_NAME);
      Path temPath = null;

      int aliveMasterCount = 0;
      // Check backup masters
      FileStatus[] files = fs.listStatus(backupPath);
      for (FileStatus status : files) {
        temPath = status.getPath();
        if (isMasterAlive(temPath.getName().replaceAll("_", ":"), conf)) {
          aliveMasterCount++;
        }
      }

      // Check active master
      files = fs.listStatus(activePath);
      if (files.length == 1) {
        temPath = files[0].getPath();
        if (isMasterAlive(temPath.getName().replaceAll("_", ":"), conf)) {
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
      e.printStackTrace();
    }
    return retValue;
  }


  public static List<String> getMasters(TajoConf conf) {
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
      e.printStackTrace();
    }
    return list;
  }

  private static FileSystem getFileSystem(TajoConf conf) throws IOException {
    Path rootPath = TajoConf.getTajoRootDir(conf);
    return rootPath.getFileSystem(conf);
  }
}
