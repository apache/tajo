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

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.exception.NoSuchDatabaseException;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.querymaster.QueryJobEvent;
import org.apache.tajo.master.rm.Worker;
import org.apache.tajo.master.rm.WorkerResource;
import org.apache.tajo.session.InvalidSessionException;
import org.apache.tajo.session.NoSuchSessionVariableException;
import org.apache.tajo.session.Session;
import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.ProtoUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueProto;
import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

public class TajoMasterClientService extends AbstractService {
  private final static Log LOG = LogFactory.getLog(TajoMasterClientService.class);
  private final MasterContext context;
  private final TajoConf conf;
  private final CatalogService catalog;
  private final TajoMasterClientProtocolServiceHandler clientHandler;
  private BlockingRpcServer server;
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
    String confClientServiceAddr = conf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confClientServiceAddr);
    int workerNum = conf.getIntVar(ConfVars.MASTER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM);
    try {
      server = new BlockingRpcServer(TajoMasterClientProtocol.class, clientHandler, initIsa, workerNum);
    } catch (Exception e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
    server.start();

    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    this.conf.setVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, NetUtils.normalizeInetSocketAddress(bindAddress));
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

  /////////////////////////////////////////////////////////////////////////////
  // TajoMasterClientProtocolService
  /////////////////////////////////////////////////////////////////////////////
  public class TajoMasterClientProtocolServiceHandler implements TajoMasterClientProtocolService.BlockingInterface {
    @Override
    public CreateSessionResponse createSession(RpcController controller, CreateSessionRequest request)
        throws ServiceException {
      try {
        // create a new session with base database name. If no database name is give, we use default database.
        String databaseName = request.hasBaseDatabaseName() ? request.getBaseDatabaseName() : DEFAULT_DATABASE_NAME;

        if (!context.getCatalog().existDatabase(databaseName)) {
          LOG.info("Session creation is canceled due to absent base database \"" + databaseName + "\".");
          throw new NoSuchDatabaseException(databaseName);
        }

        String sessionId =
            context.getSessionManager().createSession(request.getUsername(), databaseName);
        CreateSessionResponse.Builder builder = CreateSessionResponse.newBuilder();
        builder.setResultCode(ResultCode.OK);
        builder.setSessionId(TajoIdProtos.SessionIdProto.newBuilder().setId(sessionId).build());
        builder.setSessionVars(ProtoUtil.convertFromMap(context.getSessionManager().getAllVariables(sessionId)));
        return builder.build();
      } catch (NoSuchDatabaseException nsde) {
        CreateSessionResponse.Builder builder = CreateSessionResponse.newBuilder();
        builder.setResultCode(ResultCode.ERROR);
        builder.setMessage(nsde.getMessage());
        return builder.build();
      } catch (InvalidSessionException e) {
        CreateSessionResponse.Builder builder = CreateSessionResponse.newBuilder();
        builder.setResultCode(ResultCode.ERROR);
        builder.setMessage(e.getMessage());
        return builder.build();
      }
    }

    @Override
    public BoolProto removeSession(RpcController controller, TajoIdProtos.SessionIdProto request)
        throws ServiceException {

      if (request != null) {
        context.getSessionManager().removeSession(request.getId());
      }

      return BOOL_TRUE;
    }

    public SessionUpdateResponse buildSessionUpdateOnSuccess(Map<String, String> variables) {
      SessionUpdateResponse.Builder builder = SessionUpdateResponse.newBuilder();
      builder.setResultCode(ResultCode.OK);
      builder.setSessionVars(new KeyValueSet(variables).getProto());
      return builder.build();
    }

    public SessionUpdateResponse buildSessionUpdateOnError(String message) {
      SessionUpdateResponse.Builder builder = SessionUpdateResponse.newBuilder();
      builder.setResultCode(ResultCode.ERROR);
      builder.setMessage(message);
      return builder.build();
    }

    @Override
    public SessionUpdateResponse updateSessionVariables(RpcController controller, UpdateSessionVariableRequest request)
        throws ServiceException {
      try {
        String sessionId = request.getSessionId().getId();
        for (KeyValueProto kv : request.getSessionVars().getKeyvalList()) {
          context.getSessionManager().setVariable(sessionId, kv.getKey(), kv.getValue());
        }
        for (String unsetVariable : request.getUnsetVariablesList()) {
          context.getSessionManager().removeVariable(sessionId, unsetVariable);
        }
        return buildSessionUpdateOnSuccess(context.getSessionManager().getAllVariables(sessionId));
      } catch (Throwable t) {
        return buildSessionUpdateOnError("Invalid Session Id" + request.getSessionId());
      }
    }

    @Override
    public StringProto getSessionVariable(RpcController controller, SessionedStringProto request)
        throws ServiceException {

      try {
        return ProtoUtil.convertString(
            context.getSessionManager().getVariable(request.getSessionId().getId(), request.getValue()));
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public BoolProto existSessionVariable(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      try {
        String value = context.getSessionManager().getVariable(request.getSessionId().getId(), request.getValue());
        if (value != null) {
          return ProtoUtil.TRUE;
        } else {
          return ProtoUtil.FALSE;
        }
      } catch (NoSuchSessionVariableException nssv) {
        return ProtoUtil.FALSE;
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public KeyValueSetProto getAllSessionVariables(RpcController controller,
                                                                 TajoIdProtos.SessionIdProto request)
        throws ServiceException {
      try {
        String sessionId = request.getId();
        KeyValueSet keyValueSet = new KeyValueSet();
        keyValueSet.putAll(context.getSessionManager().getAllVariables(sessionId));
        return keyValueSet.getProto();
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public StringProto getCurrentDatabase(RpcController controller, TajoIdProtos.SessionIdProto request)
        throws ServiceException {
      try {
        String sessionId = request.getId();
        return ProtoUtil.convertString(context.getSessionManager().getSession(sessionId).getCurrentDatabase());
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public BoolProto selectDatabase(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {
        String sessionId = request.getSessionId().getId();
        String databaseName = request.getValue();

        if (context.getCatalog().existDatabase(databaseName)) {
          context.getSessionManager().getSession(sessionId).selectDatabase(databaseName);
          return ProtoUtil.TRUE;
        } else {
          throw new ServiceException(new NoSuchDatabaseException(databaseName));
        }
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public SubmitQueryResponse submitQuery(RpcController controller, QueryRequest request) throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        if(LOG.isDebugEnabled()) {
          LOG.debug("Query [" + request.getQuery() + "] is submitted");
        }
        return context.getGlobalEngine().executeQuery(session, request.getQuery(), request.getIsJson());
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        SubmitQueryResponse.Builder responseBuilder = ClientProtos.SubmitQueryResponse.newBuilder();
        responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
        responseBuilder.setIsForwarded(true);
        responseBuilder.setUserName(context.getConf().getVar(ConfVars.USERNAME));
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
    public UpdateQueryResponse updateQuery(RpcController controller, QueryRequest request) throws ServiceException {

      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        QueryContext queryContext = new QueryContext(conf, session);
        if (queryContext.getCurrentDatabase() == null) {
          for (Map.Entry<String,String> e : queryContext.getAllKeyValus().entrySet()) {
            System.out.println(e.getKey() + "=" + e.getValue());
          }
        }

        UpdateQueryResponse.Builder builder = UpdateQueryResponse.newBuilder();
        try {
          context.getGlobalEngine().updateQuery(queryContext, request.getQuery(), request.getIsJson());
          builder.setResultCode(ResultCode.OK);
          return builder.build();
        } catch (Exception e) {
          builder.setResultCode(ResultCode.ERROR);
          if (e.getMessage() == null) {
            builder.setErrorMessage(ExceptionUtils.getStackTrace(e));
          }
          return builder.build();
        }
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public GetQueryResultResponse getQueryResult(RpcController controller,
                                                 GetQueryResultRequest request) throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        QueryId queryId = new QueryId(request.getQueryId());
        QueryInProgress queryInProgress = context.getQueryJobManager().getQueryInProgress(queryId);

        // if we cannot get a QueryInProgress instance from QueryJobManager,
        // the instance can be in the finished query list.
        QueryInfo queryInfo = null;
        if (queryInProgress == null) {
          queryInfo = context.getQueryJobManager().getFinishedQuery(queryId);
        } else {
          queryInfo = queryInProgress.getQueryInfo();
        }

        GetQueryResultResponse.Builder builder = GetQueryResultResponse.newBuilder();
        builder.setTajoUserName(UserGroupInformation.getCurrentUser().getUserName());

        // If we cannot the QueryInfo instance from the finished list,
        // the query result was expired due to timeout.
        // In this case, we will result in error.
        if (queryInfo == null) {
          builder.setErrorMessage("No such query: " + queryId.toString());
          return builder.build();
        }

        switch (queryInfo.getQueryState()) {
          case QUERY_SUCCEEDED:
            if (queryInfo.hasResultdesc()) {
              builder.setTableDesc(queryInfo.getResultDesc().getProto());
            }
            break;
          case QUERY_FAILED:
          case QUERY_ERROR:
            builder.setErrorMessage("Query " + queryId + " is failed");
            break;
          default:
            builder.setErrorMessage("Query " + queryId + " is still running");
        }

        return builder.build();
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public GetQueryListResponse getRunningQueryList(RpcController controller, GetQueryListRequest request)

        throws ServiceException {

      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        GetQueryListResponse.Builder builder= GetQueryListResponse.newBuilder();

        Collection<QueryInProgress> queries = new ArrayList<QueryInProgress>(context.getQueryJobManager().getSubmittedQueries());
        queries.addAll(context.getQueryJobManager().getRunningQueries());
        BriefQueryInfo.Builder infoBuilder = BriefQueryInfo.newBuilder();

        for (QueryInProgress queryInProgress : queries) {
          QueryInfo queryInfo = queryInProgress.getQueryInfo();

          infoBuilder.setQueryId(queryInfo.getQueryId().getProto());
          infoBuilder.setState(queryInfo.getQueryState());
          infoBuilder.setQuery(queryInfo.getSql());
          infoBuilder.setStartTime(queryInfo.getStartTime());
          infoBuilder.setFinishTime(System.currentTimeMillis());
          infoBuilder.setProgress(queryInfo.getProgress());
          infoBuilder.setQueryMasterPort(queryInfo.getQueryMasterPort());
          infoBuilder.setQueryMasterHost(queryInfo.getQueryMasterHost());

          builder.addQueryList(infoBuilder.build());
        }

        GetQueryListResponse result = builder.build();
        return result;
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public GetQueryListResponse getFinishedQueryList(RpcController controller, GetQueryListRequest request)
        throws ServiceException {

      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        GetQueryListResponse.Builder builder = GetQueryListResponse.newBuilder();

        Collection<QueryInfo> queries
            = context.getQueryJobManager().getFinishedQueries();

        BriefQueryInfo.Builder infoBuilder = BriefQueryInfo.newBuilder();

        for (QueryInfo queryInfo : queries) {
          infoBuilder.setQueryId(queryInfo.getQueryId().getProto());
          infoBuilder.setState(queryInfo.getQueryState());
          infoBuilder.setQuery(queryInfo.getSql());
          infoBuilder.setStartTime(queryInfo.getStartTime());
          infoBuilder.setFinishTime(queryInfo.getFinishTime());
          infoBuilder.setProgress(queryInfo.getProgress());
          infoBuilder.setQueryMasterPort(queryInfo.getQueryMasterPort());
          infoBuilder.setQueryMasterHost(queryInfo.getQueryMasterHost());

          builder.addQueryList(infoBuilder.build());
        }

        GetQueryListResponse result = builder.build();
        return result;
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public GetQueryStatusResponse getQueryStatus(RpcController controller,
                                                 GetQueryStatusRequest request)
        throws ServiceException {

      try {
        context.getSessionManager().touch(request.getSessionId().getId());

        GetQueryStatusResponse.Builder builder = GetQueryStatusResponse.newBuilder();
        QueryId queryId = new QueryId(request.getQueryId());
        builder.setQueryId(request.getQueryId());

        if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
          builder.setResultCode(ResultCode.OK);
          builder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
        } else {
          QueryInProgress queryInProgress = context.getQueryJobManager().getQueryInProgress(queryId);

          // It will try to find a query status from a finished query list.
          QueryInfo queryInfo = null;
          if (queryInProgress == null) {
            queryInfo = context.getQueryJobManager().getFinishedQuery(queryId);
          } else {
            queryInfo = queryInProgress.getQueryInfo();
          }

          if (queryInfo != null) {
            builder.setResultCode(ResultCode.OK);
            builder.setState(queryInfo.getQueryState());

            boolean isCreateTable = queryInfo.getQueryContext().isCreateTable();
            boolean isInsert = queryInfo.getQueryContext().isInsert();
            builder.setHasResult(!(isCreateTable || isInsert));

            builder.setProgress(queryInfo.getProgress());
            builder.setSubmitTime(queryInfo.getStartTime());
            if(queryInfo.getQueryMasterHost() != null) {
              builder.setQueryMasterHost(queryInfo.getQueryMasterHost());
              builder.setQueryMasterPort(queryInfo.getQueryMasterClientPort());
            }
            if (queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
              builder.setFinishTime(queryInfo.getFinishTime());
            } else {
              builder.setFinishTime(System.currentTimeMillis());
            }
          } else {
            Session session = context.getSessionManager().getSession(request.getSessionId().getId());
            if (session.getNonForwardQueryResultScanner(queryId) != null) {
              builder.setResultCode(ResultCode.OK);
              builder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
            } else {
              builder.setResultCode(ResultCode.ERROR);
              builder.setErrorMessage("No such query: " + queryId.toString());
            }
          }
        }
        return builder.build();

      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public GetQueryResultDataResponse getQueryResultData(RpcController controller, GetQueryResultDataRequest request)
        throws ServiceException {
      GetQueryResultDataResponse.Builder builder = GetQueryResultDataResponse.newBuilder();
      SerializedResultSet.Builder resultSetBuilder = SerializedResultSet.newBuilder();

      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        QueryId queryId = new QueryId(request.getQueryId());
        NonForwardQueryResultScanner queryResultScanner = session.getNonForwardQueryResultScanner(queryId);
        if (queryResultScanner == null) {
          throw new ServiceException("No NonForwardQueryResultScanner for " + queryId);
        }

        List<ByteString> rows = queryResultScanner.getNextRows(request.getFetchRowNum());

        resultSetBuilder.setSchema(queryResultScanner.getLogicalSchema().getProto());
        resultSetBuilder.addAllSerializedTuples(rows);

        builder.setResultSet(resultSetBuilder.build());
        builder.setResultCode(ResultCode.OK);

        LOG.info("Send result to client for " +
            request.getSessionId().getId() + "," + queryId + ", " + rows.size() + " rows");

      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        builder.setResultSet(resultSetBuilder.build()); // required field
        builder.setResultCode(ResultCode.ERROR);
        String errorMessage = t.getMessage() == null ? t.getClass().getName() : t.getMessage();
        builder.setErrorMessage(errorMessage);
        builder.setErrorTrace(org.apache.hadoop.util.StringUtils.stringifyException(t));
      }
      return builder.build();
    }

    @Override
    public BoolProto closeNonForwardQuery(RpcController controller, QueryIdRequest request) throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        QueryId queryId = new QueryId(request.getQueryId());

        session.closeNonForwardQueryResultScanner(queryId);
        return BOOL_TRUE;
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public GetQueryInfoResponse getQueryInfo(RpcController controller, QueryIdRequest request) throws ServiceException {
      GetQueryInfoResponse.Builder builder = GetQueryInfoResponse.newBuilder();

      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        QueryId queryId = new QueryId(request.getQueryId());

        QueryManager queryManager = context.getQueryJobManager();
        QueryInProgress queryInProgress = queryManager.getQueryInProgress(queryId);

        QueryInfo queryInfo = null;
        if (queryInProgress == null) {
          queryInfo = context.getHistoryReader().getQueryInfo(queryId.toString());
        } else {
          queryInfo = queryInProgress.getQueryInfo();
        }

        if (queryInfo != null) {
          builder.setQueryInfo(queryInfo.getProto());
        }
        builder.setResultCode(ResultCode.OK);
      } catch (Throwable t) {
        LOG.warn(t.getMessage(), t);
        builder.setResultCode(ResultCode.ERROR);
        builder.setErrorMessage(org.apache.hadoop.util.StringUtils.stringifyException(t));
      }

      return builder.build();
    }

    /**
     * It is invoked by TajoContainerProxy.
     */
    @Override
    public BoolProto killQuery(RpcController controller, QueryIdRequest request) throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        QueryId queryId = new QueryId(request.getQueryId());
        QueryManager queryManager = context.getQueryJobManager();
        queryManager.getEventHandler().handle(new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_KILL,
            new QueryInfo(queryId)));
        return BOOL_TRUE;
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        throw new ServiceException(t);
      }
    }

    @Override
    public GetClusterInfoResponse getClusterInfo(RpcController controller,
                                                 GetClusterInfoRequest request)
        throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        GetClusterInfoResponse.Builder builder= GetClusterInfoResponse.newBuilder();

        Map<Integer, Worker> workers = context.getResourceManager().getWorkers();

        List<Integer> wokerKeys = new ArrayList<Integer>(workers.keySet());
        Collections.sort(wokerKeys);

        WorkerResourceInfo.Builder workerBuilder
          = WorkerResourceInfo.newBuilder();

        for(Worker worker: workers.values()) {
          WorkerResource workerResource = worker.getResource();

          workerBuilder.setConnectionInfo(worker.getConnectionInfo().getProto());
          workerBuilder.setDiskSlots(workerResource.getDiskSlots());
          workerBuilder.setCpuCoreSlots(workerResource.getCpuCoreSlots());
          workerBuilder.setMemoryMB(workerResource.getMemoryMB());
          workerBuilder.setLastHeartbeat(worker.getLastHeartbeatTime());
          workerBuilder.setUsedMemoryMB(workerResource.getUsedMemoryMB());
          workerBuilder.setUsedCpuCoreSlots(workerResource.getUsedCpuCoreSlots());
          workerBuilder.setUsedDiskSlots(workerResource.getUsedDiskSlots());
          workerBuilder.setWorkerStatus(worker.getState().toString());
          workerBuilder.setQueryMasterMode(workerResource.isQueryMasterMode());
          workerBuilder.setTaskRunnerMode(workerResource.isTaskRunnerMode());
          workerBuilder.setMaxHeap(workerResource.getMaxHeap());
          workerBuilder.setFreeHeap(workerResource.getFreeHeap());
          workerBuilder.setTotalHeap(workerResource.getTotalHeap());
          workerBuilder.setNumRunningTasks(workerResource.getNumRunningTasks());
          workerBuilder.setNumQueryMasterTasks(workerResource.getNumQueryMasterTasks());

          builder.addWorkerList(workerBuilder.build());
        }

        return builder.build();
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public BoolProto createDatabase(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        QueryContext queryContext = new QueryContext(conf, session);

        if (context.getGlobalEngine().getDDLExecutor().createDatabase(queryContext, request.getValue(), null, false)) {
          return BOOL_TRUE;
        } else {
          return BOOL_FALSE;
        }
      } catch (Throwable e) {
        throw new ServiceException(e);
      }
    }

    @Override
    public BoolProto existDatabase(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        if (catalog.existDatabase(request.getValue())) {
          return BOOL_TRUE;
        } else {
          return BOOL_FALSE;
        }
      } catch (Throwable e) {
        throw new ServiceException(e);
      }
    }

    @Override
    public BoolProto dropDatabase(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        QueryContext queryContext = new QueryContext(conf, session);

        if (context.getGlobalEngine().getDDLExecutor().dropDatabase(queryContext, request.getValue(), false)) {
          return BOOL_TRUE;
        } else {
          return BOOL_FALSE;
        }
      } catch (Throwable e) {
        throw new ServiceException(e);
      }
    }

    @Override
    public PrimitiveProtos.StringListProto getAllDatabases(RpcController controller, TajoIdProtos.SessionIdProto
        request) throws ServiceException {
      try {
        context.getSessionManager().touch(request.getId());
        return ProtoUtil.convertStrings(catalog.getAllDatabaseNames());
      } catch (Throwable e) {
        throw new ServiceException(e);
      }
    }

    @Override
    public BoolProto existTable(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        String databaseName;
        String tableName;
        if (CatalogUtil.isFQTableName(request.getValue())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getValue());
          databaseName = splitted[0];
          tableName = splitted[1];
        } else {
          databaseName = session.getCurrentDatabase();
          tableName = request.getValue();
        }

        if (databaseName == null) {
          System.out.println("A");
        }

        if (catalog.existsTable(databaseName, tableName)) {
          return BOOL_TRUE;
        } else {
          return BOOL_FALSE;
        }
      } catch (Throwable e) {
        throw new ServiceException(e);
      }
    }

    @Override
    public GetTableListResponse getTableList(RpcController controller,
                                             GetTableListRequest request) throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        String databaseName;
        if (request.hasDatabaseName()) {
          databaseName = request.getDatabaseName();
        } else {
          databaseName = session.getCurrentDatabase();
        }
        Collection<String> tableNames = catalog.getAllTableNames(databaseName);
        GetTableListResponse.Builder builder = GetTableListResponse.newBuilder();
        builder.addAllTables(tableNames);
        return builder.build();
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public TableResponse getTableDesc(RpcController controller, GetTableDescRequest request) throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        String databaseName;
        String tableName;
        if (CatalogUtil.isFQTableName(request.getTableName())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getTableName());
          databaseName = splitted[0];
          tableName = splitted[1];
        } else {
          databaseName = session.getCurrentDatabase();
          tableName = request.getTableName();
        }

        if (catalog.existsTable(databaseName, tableName)) {
          return TableResponse.newBuilder()
              .setResultCode(ResultCode.OK)
              .setTableDesc(catalog.getTableDesc(databaseName, tableName).getProto())
              .build();
        } else {
          return TableResponse.newBuilder()
              .setResultCode(ResultCode.ERROR)
              .setErrorMessage("ERROR: no such a table: " + request.getTableName())
              .build();
        }
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public TableResponse createExternalTable(RpcController controller, CreateTableRequest request)
        throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        QueryContext queryContext = new QueryContext(conf, session);

        Path path = new Path(request.getPath());
        FileSystem fs = path.getFileSystem(conf);

        if (!fs.exists(path)) {
          throw new IOException("No such a directory: " + path);
        }

        Schema schema = new Schema(request.getSchema());
        TableMeta meta = new TableMeta(request.getMeta());
        PartitionMethodDesc partitionDesc = null;
        if (request.hasPartition()) {
          partitionDesc = new PartitionMethodDesc(request.getPartition());
        }

        TableDesc desc;
        try {
          desc = context.getGlobalEngine().getDDLExecutor().createTable(queryContext, request.getName(),
              meta.getStoreType(), schema,
              meta, path, true, partitionDesc, false);
        } catch (Exception e) {
          return TableResponse.newBuilder()
              .setResultCode(ResultCode.ERROR)
              .setErrorMessage(e.getMessage()).build();
        }

        return TableResponse.newBuilder()
            .setResultCode(ResultCode.OK)
            .setTableDesc(desc.getProto()).build();
      } catch (InvalidSessionException ise) {
        return TableResponse.newBuilder()
            .setResultCode(ResultCode.ERROR)
            .setErrorMessage(ise.getMessage()).build();
      } catch (IOException ioe) {
        return TableResponse.newBuilder()
            .setResultCode(ResultCode.ERROR)
            .setErrorMessage(ioe.getMessage()).build();
      }
    }

    @Override
    public BoolProto dropTable(RpcController controller, DropTableRequest dropTable) throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(dropTable.getSessionId().getId());
        QueryContext queryContext = new QueryContext(conf, session);

        context.getGlobalEngine().getDDLExecutor().dropTable(queryContext, dropTable.getName(), false,
            dropTable.getPurge());
        return BOOL_TRUE;
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }

    @Override
    public FunctionResponse getFunctionList(RpcController controller, SessionedStringProto request)
        throws ServiceException {

      try {
        context.getSessionManager().touch(request.getSessionId().getId());

        String functionName = request.getValue();
        Collection<FunctionDesc> functions = catalog.getFunctions();

        List<CatalogProtos.FunctionDescProto> functionProtos = new ArrayList<CatalogProtos.FunctionDescProto>();

        for (FunctionDesc eachFunction: functions) {
          if (functionName == null || functionName.isEmpty()) {
            functionProtos.add(eachFunction.getProto());
          } else {
            if(functionName.equals(eachFunction.getFunctionName())) {
              functionProtos.add(eachFunction.getProto());
            }
          }
        }
        return FunctionResponse.newBuilder()
            .setResultCode(ResultCode.OK)
            .addAllFunctions(functionProtos)
            .build();
      } catch (Throwable t) {
        throw new ServiceException(t);
      }
    }
  }
}
