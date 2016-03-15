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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.CompositeService;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.json.FunctionAdapter;
import org.apache.tajo.catalog.json.TableMetaAdapter;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.function.Function;
import org.apache.tajo.json.ClassNameSerializer;
import org.apache.tajo.json.DataTypeAdapter;
import org.apache.tajo.json.DatumAdapter;
import org.apache.tajo.json.GsonSerDerAdapter;
import org.apache.tajo.json.PathSerializer;
import org.apache.tajo.json.TimeZoneGsonSerdeAdapter;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.serder.EvalNodeAdapter;
import org.apache.tajo.plan.serder.LogicalNodeAdapter;
import org.apache.tajo.ws.rs.netty.NettyRestServer;
import org.apache.tajo.ws.rs.netty.NettyRestServerFactory;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class TajoRestService extends CompositeService {
  
  private final Log LOG = LogFactory.getLog(getClass());

  private MasterContext masterContext;
  private NettyRestServer restServer;

  public TajoRestService(MasterContext masterContext) {
    super(TajoRestService.class.getName());
    
    this.masterContext = masterContext;
  }
  
  private Map<Type, GsonSerDerAdapter<?>> registerTypeAdapterMap() {
    Map<Type, GsonSerDerAdapter<?>> adapters = new HashMap<>();
    adapters.put(Path.class, new PathSerializer());
    adapters.put(Class.class, new ClassNameSerializer());
    adapters.put(LogicalNode.class, new LogicalNodeAdapter());
    adapters.put(EvalNode.class, new EvalNodeAdapter());
    adapters.put(TableMeta.class, new TableMetaAdapter());
    adapters.put(Function.class, new FunctionAdapter());
    adapters.put(GeneralFunction.class, new FunctionAdapter());
    adapters.put(AggFunction.class, new FunctionAdapter());
    adapters.put(Datum.class, new DatumAdapter());
    adapters.put(DataType.class, new DataTypeAdapter());
    adapters.put(TimeZone.class, new TimeZoneGsonSerdeAdapter());

    return adapters;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    GsonFeature gsonFeature = new GsonFeature(registerTypeAdapterMap());
    
    ClientApplication clientApplication = new ClientApplication(masterContext);
    ResourceConfig resourceConfig = ResourceConfig.forApplication(clientApplication)
        .register(gsonFeature)
        .register(LoggingFilter.class)
        .property(ServerProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
        .property(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);
    TajoConf tajoConf = (TajoConf) conf;

    InetSocketAddress address = tajoConf.getSocketAddrVar(TajoConf.ConfVars.REST_SERVICE_ADDRESS);
    URI restServiceURI = new URI("http", null, address.getHostName(), address.getPort(), "/rest", null, null);
    int workerCount = TajoConf.getIntVar(tajoConf, TajoConf.ConfVars.REST_SERVICE_RPC_SERVER_WORKER_THREAD_NUM);
    restServer = NettyRestServerFactory.createNettyRestServer(restServiceURI, resourceConfig, workerCount, false);

    super.serviceInit(conf);
    
    LOG.info("Tajo Rest Service initialized.");
  }

  @Override
  protected void serviceStart() throws Exception {
    restServer.start();

    super.serviceStart();
    
    LOG.info("Tajo Rest Service started.");
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();

    if (restServer != null) {
      restServer.shutdown();
    }
    
    LOG.info("Tajo Rest Service stopped.");
  }
  
  public InetSocketAddress getBindAddress() {
    return restServer.getListenAddress();
  }
  
}
