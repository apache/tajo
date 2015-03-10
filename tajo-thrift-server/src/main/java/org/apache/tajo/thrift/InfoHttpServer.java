package org.apache.tajo.thrift; /**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.conf.TajoConf;
import org.mortbay.jetty.Connector;

import java.io.IOException;

public class InfoHttpServer extends ThriftHttpServer {
  private static InfoHttpServer instance = null;

  private InfoHttpServer(Object containerObject , String name, String bindAddress, int port,
                           boolean findPort, Connector connector, Configuration conf,
                           String[] pathSpecs) throws IOException {
    super( name, bindAddress, port, findPort, connector, conf, pathSpecs);
  }

  public static InfoHttpServer getInstance() {
    return instance;
  }
  public static InfoHttpServer getInstance(TajoThriftServer containerObject, String name,
                                           String bindAddress, int port, boolean findPort, Connector connector,
                                           TajoConf conf,
                                           String[] pathSpecs) throws IOException {
    String addr = bindAddress;
    if(instance == null) {
      instance = new InfoHttpServer(containerObject, name, addr, port,
          findPort, connector, conf, pathSpecs);
      instance.setAttribute("tajo.thrift.info.server.object", containerObject);
      instance.setAttribute("tajo.thrift.info.server.addr", addr);
      instance.setAttribute("tajo.thrift.info.server.conf", conf);
      instance.setAttribute("tajo.thrift.info.server.starttime", System.currentTimeMillis());
    }
    return instance;
  }

  public void set(String name, Object object) {
    instance.setAttribute(name, object);
  }
}
