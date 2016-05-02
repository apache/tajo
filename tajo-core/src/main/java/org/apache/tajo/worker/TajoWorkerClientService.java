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

package org.apache.tajo.worker;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.ReturnStateUtil;
import org.apache.tajo.ipc.ClientProtos.GetQueryHistoryResponse;
import org.apache.tajo.ipc.ClientProtos.QueryIdRequest;
import org.apache.tajo.ipc.QueryMasterClientProtocol;
import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.history.QueryHistory;

import java.net.InetSocketAddress;

@Deprecated
public class TajoWorkerClientService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(TajoWorkerClientService.class);

  private BlockingRpcServer rpcServer;
  private InetSocketAddress bindAddr;

  private TajoWorker.WorkerContext workerContext;
  private TajoWorkerClientProtocolServiceHandler serviceHandler;

  public TajoWorkerClientService(TajoWorker.WorkerContext workerContext) {
    super(TajoWorkerClientService.class.getName());

    this.workerContext = workerContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    TajoConf tajoConf = TUtil.checkTypeAndGet(conf, TajoConf.class);

    this.serviceHandler = new TajoWorkerClientProtocolServiceHandler();

    // init RPC Server in constructor cause Heartbeat Thread use bindAddr
    try {
      InetSocketAddress initIsa = tajoConf.getSocketAddrVar(TajoConf.ConfVars.WORKER_CLIENT_RPC_ADDRESS);
      if (initIsa.getAddress() == null) {
        throw new IllegalArgumentException("Failed resolve of " + initIsa);
      }

      int workerNum = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM);
      this.rpcServer = new BlockingRpcServer(QueryMasterClientProtocol.class, serviceHandler, initIsa, workerNum);
      this.rpcServer.start();

      this.bindAddr = NetUtils.getConnectAddress(rpcServer.getListenAddress());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    // Get the master address
    LOG.info(TajoWorkerClientService.class.getSimpleName() + " is bind to " + bindAddr);
    tajoConf.setVar(TajoConf.ConfVars.WORKER_CLIENT_RPC_ADDRESS, NetUtils.getHostPortString(bindAddr));
    super.serviceInit(tajoConf);
  }

  @Override
  public void serviceStop() throws Exception {
    LOG.info("TajoWorkerClientService stopping");
    if(rpcServer != null) {
      rpcServer.shutdown();
    }
    LOG.info("TajoWorkerClientService stopped");
    super.serviceStop();
  }

  public InetSocketAddress getBindAddr() {
    return bindAddr;
  }

  public class TajoWorkerClientProtocolServiceHandler
          implements QueryMasterClientProtocol.QueryMasterClientProtocolService.BlockingInterface {

    @Override
    public GetQueryHistoryResponse getQueryHistory(RpcController controller, QueryIdRequest request) throws ServiceException {
      GetQueryHistoryResponse.Builder builder = GetQueryHistoryResponse.newBuilder();

      try {
        QueryId queryId = new QueryId(request.getQueryId());
        QueryHistory queryHistory = workerContext.getQueryMaster().getQueryHistory(queryId);

        if (queryHistory != null) {
          builder.setQueryHistory(queryHistory.getProto());
        }
        builder.setState(ReturnStateUtil.OK);
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        builder.setState(ReturnStateUtil.returnError(t));
      }

      return builder.build();
    }
  }
}
