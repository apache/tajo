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

package org.apache.tajo.master.querymaster;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.QueryMasterManagerProtocol;
import org.apache.tajo.ipc.QueryMasterManagerProtocol.QueryHeartbeat;
import org.apache.tajo.ipc.QueryMasterManagerProtocol.QueryHeartbeatResponse;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.rpc.ProtoBlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import org.apache.tajo.util.NetUtils;

import java.net.InetSocketAddress;

import static org.apache.tajo.conf.TajoConf.ConfVars;

public class QueryMasterManagerService extends AbstractService {
  private final static Log LOG = LogFactory.getLog(QueryMasterManagerService.class);

  private final TajoMaster.MasterContext context;
  private final TajoConf conf;
  private final QueryMasterManagerProtocolServiceHandler masterHandler;
  private ProtoBlockingRpcServer server;
  private InetSocketAddress bindAddress;

  private final BoolProto BOOL_TRUE = BoolProto.newBuilder().setValue(true).build();
  private final BoolProto BOOL_FALSE = BoolProto.newBuilder().setValue(false).build();

  public QueryMasterManagerService(TajoMaster.MasterContext context) {
    super(QueryMasterManagerService.class.getName());
    this.context = context;
    this.conf = context.getConf();
    this.masterHandler = new QueryMasterManagerProtocolServiceHandler();
  }

  @Override
  public void start() {
    // TODO resolve hostname
    String confMasterServiceAddr = conf.getVar(ConfVars.QUERY_MASTER_MANAGER_SERVICE_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterServiceAddr);
    try {
      server = new ProtoBlockingRpcServer(QueryMasterManagerProtocol.class, masterHandler, initIsa);
    } catch (Exception e) {
      LOG.error(e);
    }
    server.start();
    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    this.conf.setVar(ConfVars.QUERY_MASTER_MANAGER_SERVICE_ADDRESS, NetUtils.normalizeInetSocketAddress(bindAddress));
    LOG.info("QueryMasterManagerService startup");
    super.start();
  }

  @Override
  public void stop() {
    if(server != null) {
      server.shutdown();
      server = null;
    }
    LOG.info("QueryMasterManagerService shutdown");
    super.stop();
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  public class QueryMasterManagerProtocolServiceHandler implements QueryMasterManagerProtocol.QueryMasterManagerProtocolService.BlockingInterface {
    @Override
    public QueryHeartbeatResponse queryHeartbeat(RpcController controller, QueryHeartbeat request) throws ServiceException {
      // TODO - separate QueryMasterManagerProtocol, ClientServiceProtocol
      QueryId queryId = new QueryId(request.getQueryId());
      if(LOG.isDebugEnabled()) {
        LOG.debug("Received QueryHeartbeat:" + queryId + "," + request);
      }
      QueryMasterManager queryMasterManager = context.getQuery(queryId);
      if (queryMasterManager == null) {
        LOG.warn("No query:" + queryId);
        return QueryHeartbeatResponse.newBuilder().setHeartbeatResult(BOOL_FALSE).build();
      }

      QueryHeartbeatResponse.ResponseCommand command = queryMasterManager.queryHeartbeat(request);

      //ApplicationAttemptId attemptId = queryMasterManager.getAppAttemptId();
      //String attemptIdStr = attemptId == null ? null : attemptId.toString();
      QueryHeartbeatResponse.Builder builder = QueryHeartbeatResponse.newBuilder();
      builder.setHeartbeatResult(BOOL_TRUE);
      if(command != null) {
        builder.setResponseCommand(command);
      }
      return builder.build();
    }
  }
}
