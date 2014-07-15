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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;

import java.net.InetSocketAddress;

public class HAUtil {

  private final static int MASTER_UMBILICAL_RPC_ADDRESS = 1;
  private final static int MASTER_CLIENT_RPC_ADDRESS = 2;
  private final static int RESOURCE_TRACKER_RPC_ADDRESS = 3;
  private final static int CATALOG_ADDRESS = 4;

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

  public static InetSocketAddress getMasterAddress(TajoConf conf, int type) {
    InetSocketAddress masterAddress = null;

    if (conf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
      try {
        // Get Tajo root dir
        Path rootPath = TajoConf.getTajoRootDir(conf);

        // Check Tajo root dir
        FileSystem fs = rootPath.getFileSystem(conf);

        // Check and create Tajo system HA dir
        Path haPath = TajoConf.getSystemHADir(conf);
        Path activePath = new Path(haPath, TajoConstants.SYSTEM_HA_ACTIVE_DIR_NAME);

        if (fs.exists(activePath)) {
          FileStatus[] files = fs.listStatus(activePath);

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
        default:
          break;
      }
    }

    return masterAddress;
  }
}
