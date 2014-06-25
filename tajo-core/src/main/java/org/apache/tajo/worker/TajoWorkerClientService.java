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

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.QueryMasterClientProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.querymaster.Query;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

public class TajoWorkerClientService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(TajoWorkerClientService.class);
  private final PrimitiveProtos.BoolProto BOOL_TRUE =
          PrimitiveProtos.BoolProto.newBuilder().setValue(true).build();
  private final PrimitiveProtos.BoolProto BOOL_FALSE =
          PrimitiveProtos.BoolProto.newBuilder().setValue(false).build();

  private BlockingRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private int port;
  private TajoConf conf;
  private TajoWorker.WorkerContext workerContext;
  private TajoWorkerClientProtocolServiceHandler serviceHandler;

  public TajoWorkerClientService(TajoWorker.WorkerContext workerContext, int port) {
    super(TajoWorkerClientService.class.getName());

    this.port = port;
    this.workerContext = workerContext;
  }

  @Override
  public void init(Configuration conf) {
    Preconditions.checkArgument(conf instanceof TajoConf);
    this.conf = (TajoConf) conf;
    this.serviceHandler = new TajoWorkerClientProtocolServiceHandler();

    // init RPC Server in constructor cause Heartbeat Thread use bindAddr
    // Setup RPC server
    try {
      // TODO initial port num is value of config and find unused port with sequence
      InetSocketAddress initIsa = new InetSocketAddress("0.0.0.0", port);
      if (initIsa.getAddress() == null) {
        throw new IllegalArgumentException("Failed resolve of " + initIsa);
      }

      // TODO blocking/non-blocking??
      int workerNum = this.conf.getIntVar(TajoConf.ConfVars.WORKER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM);
      this.rpcServer = new BlockingRpcServer(QueryMasterClientProtocol.class, serviceHandler, initIsa, workerNum);
      this.rpcServer.start();

      this.bindAddr = NetUtils.getConnectAddress(rpcServer.getListenAddress());
      this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();

      this.port = bindAddr.getPort();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    // Get the master address
    LOG.info(TajoWorkerClientService.class.getSimpleName() + " is bind to " + addr);

    super.init(conf);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void stop() {
    LOG.info("TajoWorkerClientService stopping");
    if(rpcServer != null) {
      rpcServer.shutdown();
    }
    LOG.info("TajoWorkerClientService stopped");
    super.stop();
  }

  public InetSocketAddress getBindAddr() {
    return bindAddr;
  }

  public class TajoWorkerClientProtocolServiceHandler
          implements QueryMasterClientProtocol.QueryMasterClientProtocolService.BlockingInterface {
    @Override
    public PrimitiveProtos.BoolProto updateSessionVariables(
            RpcController controller,
            ClientProtos.UpdateSessionVariableRequest request) throws ServiceException {
      return null;
    }

    @Override
    public ClientProtos.GetQueryResultResponse getQueryResult(
            RpcController controller,
            ClientProtos.GetQueryResultRequest request) throws ServiceException {
      QueryId queryId = new QueryId(request.getQueryId());
      Query query = workerContext.getQueryMaster().getQueryMasterTask(queryId, true).getQuery();

      ClientProtos.GetQueryResultResponse.Builder builder = ClientProtos.GetQueryResultResponse.newBuilder();
      try {
        builder.setTajoUserName(UserGroupInformation.getCurrentUser().getUserName());
      } catch (IOException e) {
        LOG.warn("Can't get current user name");
      }

      if(query == null) {
        builder.setErrorMessage("No Query for " + queryId);
      } else {
        switch (query.getState()) {
          case QUERY_SUCCEEDED:
            builder.setTableDesc(query.getResultDesc().getProto());
            break;
          case QUERY_FAILED:
          case QUERY_ERROR:
            builder.setErrorMessage("Query " + queryId + " is failed");
          default:
            builder.setErrorMessage("Query " + queryId + " is still running");
        }
      }
      return builder.build();
    }

    @Override
    public ClientProtos.GetQueryStatusResponse getQueryStatus(
            RpcController controller,
            ClientProtos.GetQueryStatusRequest request) throws ServiceException {
      ClientProtos.GetQueryStatusResponse.Builder builder
              = ClientProtos.GetQueryStatusResponse.newBuilder();
      QueryId queryId = new QueryId(request.getQueryId());

      builder.setQueryId(request.getQueryId());

      if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
        builder.setResultCode(ClientProtos.ResultCode.OK);
        builder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
      } else {
        QueryMasterTask queryMasterTask = workerContext.getQueryMaster().getQueryMasterTask(queryId);

        builder.setResultCode(ClientProtos.ResultCode.OK);
        builder.setQueryMasterHost(bindAddr.getHostName());
        builder.setQueryMasterPort(bindAddr.getPort());

        if (queryMasterTask == null) {
          queryMasterTask = workerContext.getQueryMaster().getQueryMasterTask(queryId, true);
        }
        if (queryMasterTask == null) {
          builder.setState(TajoProtos.QueryState.QUERY_NOT_ASSIGNED);
          return builder.build();
        }

        builder.setHasResult(
            !(queryMasterTask.getQueryTaskContext().getQueryContext().isCreateTable() ||
                queryMasterTask.getQueryTaskContext().getQueryContext().isInsert())
        );

        queryMasterTask.touchSessionTime();
        Query query = queryMasterTask.getQuery();

        if (query != null) {
          builder.setState(query.getState());
          builder.setProgress(query.getProgress());
          builder.setSubmitTime(query.getAppSubmitTime());
          if (query.getState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
            builder.setFinishTime(query.getFinishTime());
          } else {
            builder.setFinishTime(System.currentTimeMillis());
          }
        } 
        Collection<TajoWorkerProtocol.TaskFatalErrorReport> diagnostics = queryMasterTask.getDiagnostics();
        if(!diagnostics.isEmpty()) {
          TajoWorkerProtocol.TaskFatalErrorReport firstError = diagnostics.iterator().next();
          builder.setErrorMessage(firstError.getErrorMessage());
          builder.setErrorTrace(firstError.getErrorTrace());
        }

        if (queryMasterTask.isInitError()) {
          Throwable initError = queryMasterTask.getInitError();
          builder.setErrorMessage(
              initError.getMessage() == null ? initError.getClass().getName() : initError.getMessage());
          builder.setErrorTrace(StringUtils.stringifyException(initError));
          builder.setState(queryMasterTask.getState());
        }
      }
      return builder.build();
    }

    @Override
    public PrimitiveProtos.BoolProto closeQuery (
            RpcController controller,
            TajoIdProtos.QueryIdProto request) throws ServiceException {
      final QueryId queryId = new QueryId(request);
      LOG.info("Stop Query:" + queryId);
      return BOOL_TRUE;
    }
  }
}
