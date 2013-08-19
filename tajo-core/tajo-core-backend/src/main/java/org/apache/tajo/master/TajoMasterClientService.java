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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.exception.AlreadyExistsTableException;
import org.apache.tajo.catalog.exception.NoSuchTableException;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.exception.SQLSyntaxError;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.querymaster.QueryMasterManager;
import org.apache.tajo.rpc.ProtoBlockingRpcServer;
import org.apache.tajo.rpc.RemoteException;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TajoIdUtils;

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
    LOG.info("TajoMasterClientService startup");
    super.start();
  }

  @Override
  public void stop() {
    if (server != null) {
      server.shutdown();
    }
    LOG.info("TajoMasterClientService shutdown");
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
    public SubmitQueryResponse submitQuery(RpcController controller,
                                           QueryRequest request)
        throws ServiceException {

      QueryId queryId;
      SubmitQueryResponse.Builder build = SubmitQueryResponse.newBuilder();
      try {
        queryId = context.getGlobalEngine().executeQuery(request.getQuery());
      } catch (SQLSyntaxError e) {
        build.setResultCode(ResultCode.ERROR);
        build.setErrorMessage(e.getMessage());
        return build.build();

      } catch (Exception e) {
        build.setResultCode(ResultCode.ERROR);
        String msg = e.getMessage();
        if (msg == null) {
          msg = "Internal Error";
        }

        if (LOG.isDebugEnabled()) {
          LOG.error(msg, e);
        } else {
          LOG.error(msg);
        }
        build.setErrorMessage(msg);
        return build.build();
      }

      LOG.info("Query " + queryId + " is submitted");
      build.setResultCode(ResultCode.OK);
      build.setQueryId(queryId.getProto());

      return build.build();
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
      QueryMasterManager queryMasterManager = context.getQuery(queryId);

      GetQueryResultResponse.Builder builder
          = GetQueryResultResponse.newBuilder();
      switch (queryMasterManager.getState()) {
        case QUERY_SUCCEEDED:
          builder.setTableDesc((TableDescProto) queryMasterManager.getResultDesc().getProto());
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

      if (queryId.equals(TajoIdUtils.NullQueryId)) {
        builder.setResultCode(ResultCode.OK);
        builder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
      } else {
        QueryMasterManager queryMasterManager = context.getQuery(queryId);
        if (queryMasterManager != null) {
          builder.setResultCode(ResultCode.OK);
          builder.setState(queryMasterManager.getState());
          builder.setProgress(queryMasterManager.getProgress());
          builder.setSubmitTime(queryMasterManager.getAppSubmitTime());
          if(queryMasterManager.getQueryMasterHost() != null) {
            builder.setQueryMasterHost(queryMasterManager.getQueryMasterHost());
            builder.setQueryMasterPort(queryMasterManager.getQueryMasterClientPort());
          }

          if (queryMasterManager.getState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
            builder.setFinishTime(queryMasterManager.getFinishTime());
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
                               ApplicationAttemptIdProto request)
        throws ServiceException {
      QueryId queryId = new QueryId(request);
      QueryMasterManager queryMasterManager = context.getQuery(queryId);
      //queryMasterManager.handle(new QueryEvent(queryId, QueryEventType.KILL));

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

      Path path = new Path(request.getPath());

      LOG.info(path.toUri());

      TableMeta meta;
      try {
        meta = TableUtil.getTableMeta(conf, path);
      } catch (IOException e) {
        throw new RemoteException(e);
      }

      FileSystem fs;

      // for legacy table structure
      Path tablePath = new Path(path, "data");
      try {
        fs = path.getFileSystem(conf);
        if (!fs.exists(tablePath)) {
          tablePath = path;
        }
      } catch (IOException e) {
        LOG.error(e);
        return null;
      }

      if (meta.getStat() == null) {
        long totalSize = 0;
        try {
          totalSize = calculateSize(tablePath);
        } catch (IOException e) {
          LOG.error("Cannot calculate the size of the relation", e);
          return null;
        }

        meta = new TableMetaImpl(meta.getProto());
        TableStat stat = new TableStat();
        stat.setNumBytes(totalSize);
        meta.setStat(stat);
      }

      desc = new TableDescImpl(request.getName(), meta, path);
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

  private long calculateSize(Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    long totalSize = 0;
    for (FileStatus status : fs.listStatus(path)) {
      totalSize += status.getLen();
    }

    return totalSize;
  }
}
