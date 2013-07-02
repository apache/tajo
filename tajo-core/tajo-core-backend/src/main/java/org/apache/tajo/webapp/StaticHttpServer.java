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
import org.mortbay.jetty.Connector;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.TajoMaster;

import java.io.IOException;

public class StaticHttpServer extends HttpServer {
  private static StaticHttpServer instance = null;
  private TajoMaster master = null;
  
  private StaticHttpServer(TajoMaster master , String name, String bindAddress, int port,
      boolean findPort, Connector connector, Configuration conf,
      String[] pathSpecs) throws IOException {
    super( name, bindAddress, port, findPort, connector, conf, pathSpecs);
    this.master = master;
  }
  public static StaticHttpServer getInstance() {
    return instance;
  }
  public static StaticHttpServer getInstance( TajoMaster master, String name,
      String bindAddress, int port, boolean findPort, Connector connector,
      TajoConf conf,
      String[] pathSpecs) throws IOException {
    String addr = bindAddress;
    if(instance == null) {
      if(bindAddress == null || bindAddress.compareTo("") == 0) {
        addr = conf.getVar(ConfVars.TASKRUNNER_LISTENER_ADDRESS).split(":")[0];
      }
      
      instance = new StaticHttpServer(master, name, addr, port,
          findPort, connector, conf, pathSpecs);
      instance.setAttribute("tajo.master", master);
      instance.setAttribute("tajo.master.addr", addr);
      instance.setAttribute("tajo.master.conf", conf);
      instance.setAttribute("tajo.master.starttime", System.currentTimeMillis());
    }
    return instance;
  }
  public TajoMaster getMaster() {
    
    return this.master;
  }
  public void set(String name, Object object) {
    instance.setAttribute(name, object);
  }
}
