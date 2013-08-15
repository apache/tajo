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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.QueryMasterClientProtocol;
import org.apache.tajo.rpc.ProtoBlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TajoIdUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class QueryMasterClientService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(QueryMasterClientService.class);
  private final PrimitiveProtos.BoolProto BOOL_TRUE =
          PrimitiveProtos.BoolProto.newBuilder().setValue(true).build();

  private ProtoBlockingRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private QueryMaster.QueryContext queryContext;
  private QueryMasterClientProtocolServiceHandler serviceHandler;

  public QueryMasterClientService(QueryMaster.QueryContext queryContext) {
    super(QueryMasterClientService.class.getName());

    this.queryContext = queryContext;
    this.serviceHandler = new QueryMasterClientProtocolServiceHandler();

    // init RPC Server in constructor cause Heartbeat Thread use bindAddr
    // Setup RPC server
    try {
      // TODO initial port num is value of config and find unused port with sequence
      InetSocketAddress initIsa = new InetSocketAddress(InetAddress.getLocalHost(), 0);
      if (initIsa.getAddress() == null) {
        throw new IllegalArgumentException("Failed resolve of " + initIsa);
      }

      // TODO blocking/non-blocking??
      this.rpcServer = new ProtoBlockingRpcServer(QueryMasterClientProtocol.class, serviceHandler, initIsa);
      this.rpcServer.start();

      this.bindAddr = NetUtils.getConnectAddress(rpcServer.getListenAddress());
      this.addr = NetUtils.normalizeInetSocketAddress(bindAddr);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    // Get the master address
    LOG.info(QueryMasterClientService.class.getSimpleName() + " (" + queryContext.getQueryId() + ") listens on "
        + addr);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void stop() {
    if(rpcServer != null) {
      rpcServer.shutdown();
    }
    LOG.info("QueryMasterClientService stopped");
    super.stop();
  }

  public InetSocketAddress getBindAddr() {
    return bindAddr;
  }


  public class QueryMasterClientProtocolServiceHandler
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
      Query query = queryContext.getQuery();

      ClientProtos.GetQueryResultResponse.Builder builder = ClientProtos.GetQueryResultResponse.newBuilder();

      if(query == null) {
        builder.setErrorMessage("No Query for " + queryId);
      } else {
        switch (query.getState()) {
          case QUERY_SUCCEEDED:
            builder.setTableDesc((CatalogProtos.TableDescProto)query.getResultDesc().getProto());
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

      if (queryId.equals(TajoIdUtils.NullQueryId)) {
        builder.setResultCode(ClientProtos.ResultCode.OK);
        builder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
      } else {
        Query query = queryContext.getQuery();
        builder.setResultCode(ClientProtos.ResultCode.OK);
        builder.setQueryMasterHost(queryContext.getQueryMasterClientService().getBindAddr().getHostName());
        builder.setQueryMasterPort(queryContext.getQueryMasterClientService().getBindAddr().getPort());


        queryContext.touchSessionTime();
        if (query != null) {
          builder.setState(query.getState());
          builder.setProgress(query.getProgress());
          builder.setSubmitTime(query.getAppSubmitTime());
          builder.setInitTime(query.getInitializationTime());
          builder.setHasResult(!query.isCreateTableStmt());
          if (query.getState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
            builder.setFinishTime(query.getFinishTime());
          } else {
            builder.setFinishTime(System.currentTimeMillis());
          }
        } else {
          builder.setState(queryContext.getState());
        }
      }

      return builder.build();
    }

    @Override
    public PrimitiveProtos.BoolProto killQuery(
            RpcController controller,
            YarnProtos.ApplicationAttemptIdProto request) throws ServiceException {
      LOG.info("Stop QueryMaster:" + queryContext.getQueryId());
      Thread t = new Thread() {
        public void run() {
          try {
            Thread.sleep(1000);   //wait tile return to rpc response
          } catch (InterruptedException e) {
          }
          queryContext.getQueryMaster().stop();
        }
      };
      t.start();
      return BOOL_TRUE;
    }
  }
}
