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

package org.apache.tajo.master;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.exception.AlreadyExistsTableException;
import org.apache.tajo.catalog.exception.NoSuchTableException;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.master.querymaster.QueryInfo;
import org.apache.tajo.master.querymaster.QueryJobManager;
import org.apache.tajo.rpc.ProtoBlockingRpcServer;
import org.apache.tajo.rpc.RemoteException;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import org.apache.tajo.util.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

public class TajoMasterClientService extends AbstractService {
  private final static Log LOG = LogFactory.getLog(TajoMasterClientService.class);
  private final MasterContext context;
  private final TajoConf conf;
  private final CatalogService catalog;
  private final TajoMasterClientProtocolServiceHandler clientHandler;
  private ProtoBlockingRpcServer server;
  private InetSocketAddress bindAddress;

  private final BoolProto BOOL_TRUE =
      BoolProto.newBuilder().setValue(true).build();
  private final BoolProto BOOL_FALSE =
      BoolProto.newBuilder().setValue(false).build();

  public TajoMasterClientService(MasterContext context) {
    super(TajoMasterClientService.class.getName());
    this.context = context;
    this.conf = context.getConf();
    this.catalog = context.getCatalog();
    this.clientHandler = new TajoMasterClientProtocolServiceHandler();
  }

  @Override
  public void start() {

    // start the rpc server
    String confClientServiceAddr = conf.getVar(ConfVars.CLIENT_SERVICE_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confClientServiceAddr);
    try {
      server = new ProtoBlockingRpcServer(TajoMasterClientProtocol.class, clientHandler, initIsa);
    } catch (Exception e) {
      LOG.error(e);
    }
    server.start();
    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    this.conf.setVar(ConfVars.CLIENT_SERVICE_ADDRESS, NetUtils.normalizeInetSocketAddress(bindAddress));
    LOG.info("Instantiated TajoMasterClientService at " + this.bindAddress);
    super.start();
  }

  @Override
  public void stop() {
    if (server != null) {
      server.shutdown();
    }
    super.stop();
  }

  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  public int getHttpPort() {
    return 0;
  }

  /////////////////////////////////////////////////////////////////////////////
  // TajoMasterClientProtocolService
  /////////////////////////////////////////////////////////////////////////////
  public class TajoMasterClientProtocolServiceHandler implements TajoMasterClientProtocolService.BlockingInterface {
    @Override
    public BoolProto updateSessionVariables(RpcController controller,
                                            UpdateSessionVariableRequest request)
        throws ServiceException {
      return null;
    }

    @Override
    public GetQueryStatusResponse submitQuery(RpcController controller,
                                           QueryRequest request)
        throws ServiceException {

      try {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Query [" + request.getQuery() + "] is submitted");
        }
        return context.getGlobalEngine().executeQuery(request.getQuery());
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        ClientProtos.GetQueryStatusResponse.Builder responseBuilder = ClientProtos.GetQueryStatusResponse.newBuilder();
        responseBuilder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          responseBuilder.setErrorMessage(ExceptionUtils.getStackTrace(e));
        } else {
          responseBuilder.setErrorMessage("Internal Error");
        }
        return responseBuilder.build();
      }
    }

    @Override
    public UpdateQueryResponse updateQuery(RpcController controller,
                                           QueryRequest request)
        throws ServiceException {

      UpdateQueryResponse.Builder builder = UpdateQueryResponse.newBuilder();
      try {
        context.getGlobalEngine().updateQuery(request.getQuery());
        builder.setResultCode(ResultCode.OK);
        return builder.build();
      } catch (Exception e) {
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() == null) {
          builder.setErrorMessage(ExceptionUtils.getStackTrace(e));
        }
        return builder.build();
      }
    }

    @Override
    public GetQueryResultResponse getQueryResult(RpcController controller,
                                                 GetQueryResultRequest request)
        throws ServiceException {
      QueryId queryId = new QueryId(request.getQueryId());
      if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {

      }
      QueryInProgress queryInProgress = context.getQueryJobManager().getQueryInProgress(queryId);
      QueryInfo queryInfo = queryInProgress.getQueryInfo();
      GetQueryResultResponse.Builder builder
          = GetQueryResultResponse.newBuilder();
      switch (queryInfo.getQueryState()) {
        case QUERY_SUCCEEDED:
          // TODO check this logic needed
          //builder.setTableDesc((TableDescProto) queryJobManager.getResultDesc().getProto());
          break;
        case QUERY_FAILED:
        case QUERY_ERROR:
          builder.setErrorMessage("Query " + queryId + " is failed");
        default:
          builder.setErrorMessage("Query " + queryId + " is still running");
      }

      return builder.build();
    }

    @Override
    public GetQueryListResponse getQueryList(RpcController controller,
                                             GetQueryListRequest request)
        throws ServiceException {
      return null;
    }

    @Override
    public GetQueryStatusResponse getQueryStatus(RpcController controller,
                                                 GetQueryStatusRequest request)
        throws ServiceException {

      GetQueryStatusResponse.Builder builder
          = GetQueryStatusResponse.newBuilder();
      QueryId queryId = new QueryId(request.getQueryId());
      builder.setQueryId(request.getQueryId());

      if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
        builder.setResultCode(ResultCode.OK);
        builder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
      } else {
        QueryInProgress queryInProgress = context.getQueryJobManager().getQueryInProgress(queryId);
        if (queryInProgress != null) {
          QueryInfo queryInfo = queryInProgress.getQueryInfo();
          builder.setResultCode(ResultCode.OK);
          builder.setState(queryInfo.getQueryState());
          builder.setProgress(queryInfo.getProgress());
          builder.setSubmitTime(queryInfo.getStartTime());
          if(queryInfo.getQueryMasterHost() != null) {
            builder.setQueryMasterHost(queryInfo.getQueryMasterHost());
            builder.setQueryMasterPort(queryInfo.getQueryMasterClientPort());
          }
          //builder.setInitTime(queryJobManager.getInitializationTime());
          //builder.setHasResult(!queryJobManager.isCreateTableStmt());
          if (queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
            builder.setFinishTime(queryInfo.getFinishTime());
          } else {
            builder.setFinishTime(System.currentTimeMillis());
          }
        } else {
          builder.setResultCode(ResultCode.ERROR);
          builder.setErrorMessage("No such query: " + queryId.toString());
        }
      }

      return builder.build();
    }

    @Override
    public BoolProto killQuery(RpcController controller,
                               TajoIdProtos.QueryIdProto request)
        throws ServiceException {
      QueryId queryId = new QueryId(request);
      QueryJobManager queryJobManager = context.getQueryJobManager();
      //TODO KHJ, change QueryJobManager to event handler
      //queryJobManager.handle(new QueryEvent(queryId, QueryEventType.KILL));

      return BOOL_TRUE;
    }

    @Override
    public GetClusterInfoResponse getClusterInfo(RpcController controller,
                                                 GetClusterInfoRequest request)
        throws ServiceException {
      return null;
    }

    @Override
    public BoolProto existTable(RpcController controller,
                                StringProto tableNameProto)
        throws ServiceException {
      String tableName = tableNameProto.getValue();
      if (catalog.existsTable(tableName)) {
        return BOOL_TRUE;
      } else {
        return BOOL_FALSE;
      }
    }

    @Override
    public GetTableListResponse getTableList(RpcController controller,
                                             GetTableListRequest request)
        throws ServiceException {
      Collection<String> tableNames = catalog.getAllTableNames();
      GetTableListResponse.Builder builder = GetTableListResponse.newBuilder();
      builder.addAllTables(tableNames);
      return builder.build();
    }

    @Override
    public TableResponse getTableDesc(RpcController controller,
                                      GetTableDescRequest request)
        throws ServiceException {
      String name = request.getTableName();
      if (catalog.existsTable(name)) {
        return TableResponse.newBuilder()
            .setTableDesc((TableDescProto) catalog.getTableDesc(name).getProto())
            .build();
      } else {
        return null;
      }
    }

    @Override
    public TableResponse createTable(RpcController controller, CreateTableRequest request)
        throws ServiceException {
      Path path = new Path(request.getPath());
      TableMeta meta = new TableMetaImpl(request.getMeta());
      TableDesc desc;
      try {
        desc = context.getGlobalEngine().createTable(request.getName(), meta, path);
      } catch (Exception e) {
        return TableResponse.newBuilder().setErrorMessage(e.getMessage()).build();
      }

      return TableResponse.newBuilder().setTableDesc((TableDescProto) desc.getProto()).build();
    }

    @Override
    public BoolProto dropTable(RpcController controller,
                               StringProto tableNameProto)
        throws ServiceException {
      context.getGlobalEngine().dropTable(tableNameProto.getValue());
      return BOOL_TRUE;
    }

    @Override
    public TableResponse attachTable(RpcController controller,
                                     AttachTableRequest request)
        throws ServiceException {

      TableDesc desc;
      if (catalog.existsTable(request.getName())) {
        throw new AlreadyExistsTableException(request.getName());
      }

      Path tablePath = new Path(request.getPath());

      LOG.info(tablePath.toUri());

      TableMeta meta;
      try {
        meta = TableUtil.getTableMeta(conf, tablePath);
      } catch (IOException e) {
        throw new RemoteException(e);
      }

      if (meta.getStat() == null) {
        long totalSize;
        try {
          FileSystem fs = tablePath.getFileSystem(conf);
          totalSize = fs.getContentSummary(tablePath).getSpaceConsumed();
        } catch (IOException e) {
          LOG.error("Cannot get the volume of the table", e);
          return null;
        }

        meta = new TableMetaImpl(meta.getProto());
        TableStat stat = new TableStat();
        stat.setNumBytes(totalSize);
        meta.setStat(stat);
      }

      desc = new TableDescImpl(request.getName(), meta, tablePath);
      catalog.addTable(desc);
      LOG.info("Table " + desc.getName() + " is attached ("
          + meta.getStat().getNumBytes() + ")");

      return TableResponse.newBuilder().
          setTableDesc((TableDescProto) desc.getProto())
          .build();
    }

    @Override
    public BoolProto detachTable(RpcController controller,
                                 StringProto tableNameProto)
        throws ServiceException {
      String tableName = tableNameProto.getValue();
      if (!catalog.existsTable(tableName)) {
        throw new NoSuchTableException(tableName);
      }

      catalog.deleteTable(tableName);

      LOG.info("Table " + tableName + " is detached");
      return BOOL_TRUE;
    }
  }
}
