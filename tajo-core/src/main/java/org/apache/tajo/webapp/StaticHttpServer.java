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

package org.apache.tajo.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.worker.TajoWorker;
import org.mortbay.jetty.Connector;

import java.io.IOException;

public class StaticHttpServer extends HttpServer {
  private static StaticHttpServer instance = null;

  private static final Object lockObjectForStaticHttpServer = new Object();

  private StaticHttpServer(Object containerObject , String name, String bindAddress, int port,
      boolean findPort, Connector connector, Configuration conf,
      String[] pathSpecs) throws IOException {
    super( name, bindAddress, port, findPort, connector, conf, pathSpecs);
  }
  public static StaticHttpServer getInstance() {
    return instance;
  }
  public static StaticHttpServer getInstance(Object containerObject, String name,
      String bindAddress, int port, boolean findPort, Connector connector,
      TajoConf conf,
      String[] pathSpecs) throws IOException {
    String addr = bindAddress;
    if(instance == null) {
      if(bindAddress == null || bindAddress.compareTo("") == 0) {
        if (containerObject instanceof TajoMaster) {
          addr = conf.getSocketAddrVar(
              ConfVars.TAJO_MASTER_INFO_ADDRESS).getHostName();
        } else if (containerObject instanceof TajoWorker) {
          addr = conf.getSocketAddrVar(
              ConfVars.WORKER_INFO_ADDRESS).getHostName();
        }
      }

      synchronized (lockObjectForStaticHttpServer) {
        if (instance == null) {
          instance = new StaticHttpServer(containerObject, name, addr, port,
              findPort, connector, conf, pathSpecs);
          instance.setAttribute("tajo.info.server.object", containerObject);
          instance.setAttribute("tajo.info.server.addr", addr);
          instance.setAttribute("tajo.info.server.conf", conf);
          instance.setAttribute("tajo.info.server.starttime", System.currentTimeMillis());
        }
      }
    }
    return instance;
  }

  public void set(String name, Object object) {
    instance.setAttribute(name, object);
  }
}
