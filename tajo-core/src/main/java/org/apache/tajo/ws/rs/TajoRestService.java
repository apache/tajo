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

package org.apache.tajo.ws.rs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.ws.rs.netty.NettyRestServer;
import org.apache.tajo.ws.rs.netty.NettyRestServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.URI;

public class TajoRestService extends CompositeService {

  private NettyRestServer restServer;

  public TajoRestService(MasterContext masterContext) {
    super(TajoRestService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    ClientApplication clientApplication = new ClientApplication();
    ResourceConfig resourceConfig = ResourceConfig.forApplication(clientApplication);
    TajoConf tajoConf = (TajoConf) conf;

    int port = TajoConf.getIntVar(tajoConf, TajoConf.ConfVars.REST_SERVICE_PORT);
    URI restServiceURI = new URI("http", null, "localhost", port, "rest", null, null);
    int workerCount = TajoConf.getIntVar(tajoConf, TajoConf.ConfVars.REST_SERVICE_RPC_SERVER_WORKER_THREAD_NUM);
    restServer = NettyRestServerFactory.createNettyRestServer(restServiceURI, resourceConfig, workerCount, false);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    restServer.start();

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();

    restServer.shutdown();
  }
}
