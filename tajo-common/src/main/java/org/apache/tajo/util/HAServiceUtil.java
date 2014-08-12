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

package org.apache.tajo.util;

import org.apache.hadoop.fs.*;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class HAServiceUtil {

  private final static int MASTER_UMBILICAL_RPC_ADDRESS = 1;
  private final static int MASTER_CLIENT_RPC_ADDRESS = 2;
  private final static int RESOURCE_TRACKER_RPC_ADDRESS = 3;
  private final static int CATALOG_ADDRESS = 4;
  private final static int MASTER_INFO_ADDRESS = 5;

  public static InetSocketAddress getMasterUmbilicalAddress(TajoConf conf) {
    return getMasterAddress(conf, MASTER_UMBILICAL_RPC_ADDRESS);
  }

  public static String getMasterUmbilicalName(TajoConf conf) {
    return NetUtils.normalizeInetSocketAddress(getMasterUmbilicalAddress(conf));
  }

  public static InetSocketAddress getMasterClientAddress(TajoConf conf) {
    return getMasterAddress(conf, MASTER_CLIENT_RPC_ADDRESS);
  }

  public static String getMasterClientName(TajoConf conf) {
    return NetUtils.normalizeInetSocketAddress(getMasterClientAddress(conf));
  }

  public static InetSocketAddress getResourceTrackerAddress(TajoConf conf) {
    return getMasterAddress(conf, RESOURCE_TRACKER_RPC_ADDRESS);
  }

  public static String getResourceTrackerName(TajoConf conf) {
    return NetUtils.normalizeInetSocketAddress(getResourceTrackerAddress(conf));
  }

  public static InetSocketAddress getCatalogAddress(TajoConf conf) {
    return getMasterAddress(conf, CATALOG_ADDRESS);
  }

  public static String getCatalogName(TajoConf conf) {
    return NetUtils.normalizeInetSocketAddress(getCatalogAddress(conf));
  }

  public static InetSocketAddress getMasterInfoAddress(TajoConf conf) {
    return getMasterAddress(conf, MASTER_INFO_ADDRESS);
  }

  public static String getMasterInfoName(TajoConf conf) {
    return NetUtils.normalizeInetSocketAddress(getMasterInfoAddress(conf));
  }

  public static InetSocketAddress getMasterAddress(TajoConf conf, int type) {
    InetSocketAddress masterAddress = null;

    if (conf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
      try {
        FileSystem fs = getFileSystem(conf);
        Path[] paths = getSystemPath(conf);

        if (fs.exists(paths[1])) {
          FileStatus[] files = fs.listStatus(paths[1]);

          if (files.length == 1) {
            Path file = files[0].getPath();
            String hostAddress = file.getName().replaceAll("_", ":");
            FSDataInputStream stream = fs.open(file);
            String data = stream.readUTF();
            stream.close();

            String[] addresses = data.split("_");

            switch (type) {
              case 1:
                masterAddress = NetUtils.createSocketAddr(hostAddress);
                break;
              case 2:
                masterAddress = NetUtils.createSocketAddr(addresses[0]);
                break;
              case 3:
                masterAddress = NetUtils.createSocketAddr(addresses[1]);
                break;
              case 4:
                masterAddress = NetUtils.createSocketAddr(addresses[2]);
                break;
              case 5:
                masterAddress = NetUtils.createSocketAddr(addresses[3]);
                break;
              default:
                break;
            }
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    if (masterAddress == null) {
      switch (type) {
        case 1:
          masterAddress = NetUtils.createSocketAddr(conf.getVar(TajoConf.ConfVars
              .TAJO_MASTER_UMBILICAL_RPC_ADDRESS));
          break;
        case 2:
          masterAddress = NetUtils.createSocketAddr(conf.getVar(TajoConf.ConfVars
              .TAJO_MASTER_CLIENT_RPC_ADDRESS));
          break;
        case 3:
          masterAddress = NetUtils.createSocketAddr(conf.getVar(TajoConf.ConfVars
              .RESOURCE_TRACKER_RPC_ADDRESS));
          break;
        case 4:
          masterAddress = NetUtils.createSocketAddr(conf.getVar(TajoConf.ConfVars
              .CATALOG_ADDRESS));
          break;
        case 5:
          masterAddress = NetUtils.createSocketAddr(conf.getVar(TajoConf.ConfVars
              .TAJO_MASTER_INFO_ADDRESS));
          break;
        default:
          break;
      }
    }

    return masterAddress;
  }

  public static boolean isMasterAlive(InetSocketAddress masterAddress, TajoConf conf) {
    return isMasterAlive(NetUtils.normalizeInetSocketAddress(masterAddress), conf);
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
      Path[] paths = getSystemPath(conf);
      Path temPath = null;

      // Check backup masters
      FileStatus[] files = fs.listStatus(paths[2]);
      for (FileStatus status : files) {
        temPath = status.getPath();
        if (temPath.getName().equals(targetMaster)) {
          return 0;
        }
      }

      // Check active master
      files = fs.listStatus(paths[1]);
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
      Path[] paths = getSystemPath(conf);
      Path temPath = null;

      int aliveMasterCount = 0;
      // Check backup masters
      FileStatus[] files = fs.listStatus(paths[2]);
      for (FileStatus status : files) {
        temPath = status.getPath();
        if (isMasterAlive(temPath.getName().replaceAll("_", ":"), conf)) {
          aliveMasterCount++;
        }
      }

      // Check active master
      files = fs.listStatus(paths[1]);
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
      fs.delete(paths[0], true);
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
      Path[] paths = getSystemPath(conf);
      Path temPath = null;

      // Check backup masters
      FileStatus[] files = fs.listStatus(paths[2]);
      for (FileStatus status : files) {
        temPath = status.getPath();
        list.add(temPath.getName().replaceAll("_", ":"));
      }

      // Check active master
      files = fs.listStatus(paths[1]);
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

  private static Path[] getSystemPath(TajoConf conf) throws IOException {
    Path[] paths = new Path[3];
    paths[0] = TajoConf.getSystemHADir(conf);
    paths[1] = new Path(paths[0], TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);
    paths[2] = new Path(paths[0], TajoConstants.SYSTEM_HA_BACKUP_DIR_NAME);

    return paths;
  }
}
