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

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.QueryNotFoundException;
import org.apache.tajo.exception.ReturnStateUtil;
import org.apache.tajo.exception.UnavailableTableLocationException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.exec.NonForwardQueryResultFileScanner;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.master.rm.NodeStatus;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.PartitionedTableScanNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.querymaster.QueryJobEvent;
import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.*;
import org.apache.tajo.session.Session;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.ProtoUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.exception.ExceptionUtil.printStackTraceIfError;
import static org.apache.tajo.exception.ReturnStateUtil.*;

/**
 * It provides Client Remote API service for TajoMaster.
 */
public class TajoMasterClientService extends AbstractService {
  private final static Log LOG = LogFactory.getLog(TajoMasterClientService.class);
  private final MasterContext context;
  private final TajoConf conf;
  private final CatalogService catalog;
  private final TajoMasterClientProtocolServiceHandler clientHandler;
  private BlockingRpcServer server;
  private InetSocketAddress bindAddress;


  public TajoMasterClientService(MasterContext context) {
    super(TajoMasterClientService.class.getName());
    this.context = context;
    this.conf = context.getConf();
    this.catalog = context.getCatalog();
    this.clientHandler = new TajoMasterClientProtocolServiceHandler();
  }

  @Override
  public void serviceStart() throws Exception {

    // start the rpc server
    String confClientServiceAddr = conf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confClientServiceAddr);
    int workerNum = conf.getIntVar(ConfVars.MASTER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM);
    server = new BlockingRpcServer(TajoMasterClientProtocol.class, clientHandler, initIsa, workerNum);
    server.start();

    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    this.conf.setVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, NetUtils.normalizeInetSocketAddress(bindAddress));
    super.serviceStart();
    LOG.info("Instantiated TajoMasterClientService at " + this.bindAddress);
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.shutdown();
    }
    super.serviceStop();
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
          throw new UndefinedDatabaseException(databaseName);
        }

        String sessionId =
            context.getSessionManager().createSession(request.getUsername(), databaseName);
        CreateSessionResponse.Builder builder = CreateSessionResponse.newBuilder();
        builder.setState(OK);
        builder.setSessionId(TajoIdProtos.SessionIdProto.newBuilder().setId(sessionId).build());
        builder.setSessionVars(ProtoUtil.convertFromMap(context.getSessionManager().getAllVariables(sessionId)));
        return builder.build();

      } catch (Throwable t) {

        printStackTraceIfError(LOG, t);

        CreateSessionResponse.Builder builder = CreateSessionResponse.newBuilder();
        builder.setState(returnError(t));
        return builder.build();
      }
    }

    @Override
    public ReturnState removeSession(RpcController controller, TajoIdProtos.SessionIdProto request)
        throws ServiceException {

      try {
        if (request != null) {
          context.getSessionManager().removeSession(request.getId());
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return ReturnStateUtil.returnError(t);
      }

      return OK;
    }

    @Override
    public SessionUpdateResponse updateSessionVariables(RpcController controller,
                                                        UpdateSessionVariableRequest request)
        throws ServiceException {

      SessionUpdateResponse.Builder builder = SessionUpdateResponse.newBuilder();

      try {
        String sessionId = request.getSessionId().getId();
        for (KeyValueProto kv : request.getSessionVars().getKeyvalList()) {
          context.getSessionManager().setVariable(sessionId, kv.getKey(), kv.getValue());
        }
        for (String unsetVariable : request.getUnsetVariablesList()) {
          context.getSessionManager().removeVariable(sessionId, unsetVariable);
        }

        builder.setState(OK);
        builder.setSessionVars(new KeyValueSet(context.getSessionManager().getAllVariables(sessionId)).getProto());
        return builder.build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        builder.setState(returnError(t));
        return builder.build();
      }
    }

    @Override
    public StringResponse getSessionVariable(RpcController controller, SessionedStringProto request)
        throws ServiceException {

      try {
        String value = context.getSessionManager().getVariable(request.getSessionId().getId(), request.getValue());

        return StringResponse.newBuilder()
            .setState(OK)
            .setValue(value)
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return StringResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public ReturnState existSessionVariable(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      try {
        String value = context.getSessionManager().getVariable(request.getSessionId().getId(), request.getValue());
        if (value != null) {
          return OK;
        } else {
          return errNoSessionVar(request.getValue());
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public KeyValueSetResponse getAllSessionVariables(RpcController controller,
                                                                 TajoIdProtos.SessionIdProto request)
        throws ServiceException {

      try {
        String sessionId = request.getId();
        KeyValueSet keyValueSet = new KeyValueSet();
        keyValueSet.putAll(context.getSessionManager().getAllVariables(sessionId));

        return KeyValueSetResponse.newBuilder()
            .setState(OK)
            .setValue(keyValueSet.getProto())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return KeyValueSetResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public StringResponse getCurrentDatabase(RpcController controller, TajoIdProtos.SessionIdProto request)
        throws ServiceException {
      try {
        return StringResponse.newBuilder()
            .setState(OK)
            .setValue(context.getSessionManager().getSession(request.getId()).getCurrentDatabase())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return StringResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public ReturnState selectDatabase(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {
        String sessionId = request.getSessionId().getId();
        String databaseName = request.getValue();

        if (context.getCatalog().existDatabase(databaseName)) {
          context.getSessionManager().getSession(sessionId).selectDatabase(databaseName);
          return OK;
        } else {
          return errUndefinedDatabase(databaseName);
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
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

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return ClientProtos.SubmitQueryResponse.newBuilder()
            .setState(returnError(t))
            .setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto())
            .setUserName(context.getConf().getVar(ConfVars.USERNAME))
            .build();
      }
    }

    @Override
    public UpdateQueryResponse updateQuery(RpcController controller, QueryRequest request) throws ServiceException {
      UpdateQueryResponse.Builder builder = UpdateQueryResponse.newBuilder();

      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        QueryContext queryContext = new QueryContext(conf, session);
        context.getGlobalEngine().updateQuery(queryContext, request.getQuery(), request.getIsJson());
        builder.setState(OK);

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        builder.setState(returnError(t));
      }
      return builder.build();
    }

    @Override
    public GetQueryResultResponse getQueryResult(RpcController controller,
                                                 GetQueryResultRequest request) throws ServiceException {

      GetQueryResultResponse.Builder builder = GetQueryResultResponse.newBuilder();

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

        builder.setTajoUserName(UserGroupInformation.getCurrentUser().getUserName());

        // If we cannot the QueryInfo instance from the finished list,
        // the query result was expired due to timeout.
        // In this case, we will result in error.
        if (queryInfo == null) {
          builder.setState(errNoSuchQueryId(queryId));
          return builder.build();
        }

        switch (queryInfo.getQueryState()) {
          case QUERY_SUCCEEDED:
            if (queryInfo.hasResultdesc()) {
              builder.setState(OK);
              builder.setTableDesc(queryInfo.getResultDesc().getProto());
            }
            break;
          case QUERY_FAILED:
          case QUERY_ERROR:
            builder.setState(errNoData(queryId));
            break;

          default:
            builder.setState(errIncompleteQuery(queryId));
        }

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        builder.setState(returnError(t));
      }

      return builder.build();
    }

    @Override
    public GetQueryListResponse getRunningQueryList(RpcController controller, TajoIdProtos.SessionIdProto request)
        throws ServiceException {

      GetQueryListResponse.Builder builder = GetQueryListResponse.newBuilder();

      try {
        context.getSessionManager().touch(request.getId());

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
          if(queryInfo.getQueryMasterHost() != null){
            infoBuilder.setQueryMasterPort(queryInfo.getQueryMasterPort());
            infoBuilder.setQueryMasterHost(queryInfo.getQueryMasterHost());
          }

          builder.addQueryList(infoBuilder.build());
        }

        builder.setState(OK);

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        builder.setState(returnError(t));
      }

      return builder.build();
    }

    @Override
    public GetQueryListResponse getFinishedQueryList(RpcController controller, TajoIdProtos.SessionIdProto request)
        throws ServiceException {

      GetQueryListResponse.Builder builder = GetQueryListResponse.newBuilder();

      try {
        context.getSessionManager().touch(request.getId());

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
          if(queryInfo.getQueryMasterHost() != null){
            infoBuilder.setQueryMasterPort(queryInfo.getQueryMasterPort());
            infoBuilder.setQueryMasterHost(queryInfo.getQueryMasterHost());
          }

          builder.addQueryList(infoBuilder.build());
        }

        builder.setState(OK);

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        builder.setState(returnError(t));
      }

      return builder.build();
    }

    @Override
    public GetQueryStatusResponse getQueryStatus(RpcController controller, GetQueryStatusRequest request)
        throws ServiceException {

      GetQueryStatusResponse.Builder builder = GetQueryStatusResponse.newBuilder();

      try {
        context.getSessionManager().touch(request.getSessionId().getId());

        QueryId queryId = new QueryId(request.getQueryId());
        builder.setQueryId(request.getQueryId());

        if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
          builder.setState(OK);
          builder.setQueryState(QueryState.QUERY_SUCCEEDED);
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
            builder.setState(OK);
            builder.setQueryState(queryInfo.getQueryState());

            boolean isCreateTable = queryInfo.getQueryContext().isCreateTable();
            boolean isInsert = queryInfo.getQueryContext().isInsert();
            builder.setHasResult(!(isCreateTable || isInsert));

            builder.setProgress(queryInfo.getProgress());
            builder.setSubmitTime(queryInfo.getStartTime());
            if(queryInfo.getQueryMasterHost() != null) {
              builder.setQueryMasterHost(queryInfo.getQueryMasterHost());
              builder.setQueryMasterPort(queryInfo.getQueryMasterClientPort());
            }
            if (queryInfo.getQueryState() == QueryState.QUERY_SUCCEEDED) {
              builder.setFinishTime(queryInfo.getFinishTime());
            } else {
              builder.setFinishTime(System.currentTimeMillis());
            }
          } else {
            Session session = context.getSessionManager().getSession(request.getSessionId().getId());
            if (session.getNonForwardQueryResultScanner(queryId) != null) {
              builder.setState(OK);
              builder.setQueryState(QueryState.QUERY_SUCCEEDED);
            } else {
              builder.setState(errNoSuchQueryId(queryId));
            }
          }
        }

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        builder.setState(returnError(t));
      }

      return builder.build();
    }

    @Override
    public GetQueryResultDataResponse getQueryResultData(RpcController controller, GetQueryResultDataRequest request)
        throws ServiceException {
      GetQueryResultDataResponse.Builder builder = GetQueryResultDataResponse.newBuilder();

      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        QueryId queryId = new QueryId(request.getQueryId());
        NonForwardQueryResultScanner queryResultScanner = session.getNonForwardQueryResultScanner(queryId);

        if (queryResultScanner == null) {

          QueryInfo queryInfo = context.getQueryJobManager().getFinishedQuery(queryId);
          if (queryInfo == null) {
            throw new QueryNotFoundException(queryId.toString());
          }

          TableDesc resultTableDesc = queryInfo.getResultDesc();
          Preconditions.checkNotNull(resultTableDesc, "QueryInfo::getResultDesc results in NULL.");

          ScanNode scanNode;
          if (resultTableDesc.hasPartition()) {
            scanNode = LogicalPlan.createNodeWithoutPID(PartitionedTableScanNode.class);
            scanNode.init(resultTableDesc);
          } else {
            scanNode = LogicalPlan.createNodeWithoutPID(ScanNode.class);
            scanNode.init(resultTableDesc);
          }

          if(request.hasCompressCodec()) {
            queryResultScanner = new NonForwardQueryResultFileScanner(context.getConf(), session.getSessionId(),
                queryId, scanNode, resultTableDesc, Integer.MAX_VALUE, request.getCompressCodec());
          } else {
            queryResultScanner = new NonForwardQueryResultFileScanner(context.getConf(),
                session.getSessionId(), queryId, scanNode, resultTableDesc, Integer.MAX_VALUE);
          }

          queryResultScanner.init();
          session.addNonForwardQueryResultScanner(queryResultScanner);
        }

        SerializedResultSet resultSet = queryResultScanner.nextRowBlock(request.getFetchRowNum());

        builder.setResultSet(resultSet);
        builder.setState(OK);
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        builder.setState(returnError(t));
      }

      return builder.build();
    }

    @Override
    public ReturnState closeNonForwardQuery(RpcController controller, QueryIdRequest request)
        throws ServiceException {

      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        QueryId queryId = new QueryId(request.getQueryId());

        session.closeNonForwardQueryResultScanner(queryId);
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public GetQueryInfoResponse getQueryInfo(RpcController controller, QueryIdRequest request) {
      GetQueryInfoResponse.Builder builder = GetQueryInfoResponse.newBuilder();

      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        QueryId queryId = new QueryId(request.getQueryId());

        QueryManager queryManager = context.getQueryJobManager();
        QueryInProgress queryInProgress = queryManager.getQueryInProgress(queryId);

        QueryInfo queryInfo;
        if (queryInProgress == null) {
          queryInfo = context.getQueryJobManager().getFinishedQuery(queryId);
        } else {
          queryInfo = queryInProgress.getQueryInfo();
        }

        if (queryInfo == null) {
          throw new QueryNotFoundException(queryId.toString());
        }

        builder.setQueryInfo(queryInfo.getProto());
        builder.setState(OK);

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        builder.setState(returnError(t));
      }

      return builder.build();
    }

    @Override
    public ReturnState killQuery(RpcController controller, QueryIdRequest request) throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        QueryId queryId = new QueryId(request.getQueryId());

        QueryInProgress progress = context.getQueryJobManager().getQueryInProgress(queryId);
        if (progress == null || progress.isFinishState() || progress.isKillWait()) {
          return OK;
        }

        QueryManager queryManager = context.getQueryJobManager();
        queryManager.getEventHandler().handle(new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_KILL,
            new QueryInfo(queryId)));

        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public GetClusterInfoResponse getClusterInfo(RpcController controller,
                                                 GetClusterInfoRequest request)
        throws ServiceException {

      GetClusterInfoResponse.Builder builder = GetClusterInfoResponse.newBuilder();

      try {
        context.getSessionManager().touch(request.getSessionId().getId());

        List<NodeStatus> nodeStatusList = new ArrayList<NodeStatus>(context.getResourceManager().getRMContext().getNodes().values());
        Collections.sort(nodeStatusList);

        WorkerResourceInfo.Builder workerBuilder = WorkerResourceInfo.newBuilder();

        for(NodeStatus nodeStatus : nodeStatusList) {
          workerBuilder.setConnectionInfo(nodeStatus.getConnectionInfo().getProto());
          workerBuilder.setAvailableResource(nodeStatus.getAvailableResource().getProto());
          workerBuilder.setTotalResource(nodeStatus.getTotalResourceCapability().getProto());

          workerBuilder.setLastHeartbeat(nodeStatus.getLastHeartbeatTime());
          workerBuilder.setWorkerStatus(nodeStatus.getState().toString());
          workerBuilder.setNumRunningTasks(nodeStatus.getNumRunningTasks());
          workerBuilder.setNumQueryMasterTasks(nodeStatus.getNumRunningQueryMaster());

          builder.addWorkerList(workerBuilder.build());
        }

        builder.setState(OK);

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        builder.setState(returnError(t));
      }

      return builder.build();
    }

    @Override
    public ReturnState createDatabase(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {
        final Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        final QueryContext queryContext = new QueryContext(conf, session);

        context.getGlobalEngine().getDDLExecutor().createDatabase(queryContext, request.getValue(), null, false);
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public ReturnState existDatabase(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        if (catalog.existDatabase(request.getValue())) {
          return OK;
        } else {
          return errUndefinedDatabase(request.getValue());
        }

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public ReturnState dropDatabase(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {
        final Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        final QueryContext queryContext = new QueryContext(conf, session);

        context.getGlobalEngine().getDDLExecutor().dropDatabase(queryContext, request.getValue(), false);
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public StringListResponse getAllDatabases(RpcController controller, TajoIdProtos.SessionIdProto
        request) throws ServiceException {

      try {
        context.getSessionManager().touch(request.getId());

        return StringListResponse.newBuilder()
            .setState(OK)
            .addAllValues(catalog.getAllDatabaseNames())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return StringListResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public ReturnState existTable(RpcController controller, SessionedStringProto request) throws ServiceException {

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

        if (catalog.existsTable(databaseName, tableName)) {
          return OK;
        } else {
          return errUndefinedTable(tableName);
        }

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public StringListResponse getTableList(RpcController controller,
                                             SessionedStringProto request) throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        String databaseName;
        if (request.hasValue()) {
          databaseName = request.getValue();
        } else {
          databaseName = session.getCurrentDatabase();
        }
        Collection<String> tableNames = catalog.getAllTableNames(databaseName);

        return StringListResponse.newBuilder()
          .setState(OK)
          .addAllValues(tableNames)
          .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return StringListResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public TableResponse getTableDesc(RpcController controller, SessionedStringProto request) throws ServiceException {
      try {

        if (!request.hasValue()) {
          return TableResponse.newBuilder()
              .setState(errInvalidRpcCall("Table name is required"))
              .build();
        }

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

        if (catalog.existsTable(databaseName, tableName)) {
          return TableResponse.newBuilder()
              .setState(OK)
              .setTable(catalog.getTableDesc(databaseName, tableName).getProto())
              .build();
        } else {
          return TableResponse.newBuilder()
              .setState(errUndefinedTable(request.getValue()))
              .build();
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return TableResponse.newBuilder()
            .setState(returnError(t))
            .build();
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
          throw new UnavailableTableLocationException(path.toString(), "no such a directory");
        }

        Schema schema = null;
        if (request.hasSchema()) {
          schema = new Schema(request.getSchema());
        }

        TableMeta meta = new TableMeta(request.getMeta());
        PartitionMethodDesc partitionDesc = null;
        if (request.hasPartition()) {
          partitionDesc = new PartitionMethodDesc(request.getPartition());
        }

        TableDesc desc = context.getGlobalEngine().getDDLExecutor().getCreateTableExecutor().create(
            queryContext,
            request.getName(),
            null,
            schema,
            meta,
            path.toUri(),
            true,
            partitionDesc,
            false
        );

        return TableResponse.newBuilder()
            .setState(OK)
            .setTable(desc.getProto()).build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return TableResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public ReturnState dropTable(RpcController controller, DropTableRequest dropTable) throws ServiceException {
      try {
        Session session = context.getSessionManager().getSession(dropTable.getSessionId().getId());
        QueryContext queryContext = new QueryContext(conf, session);

        context.getGlobalEngine().getDDLExecutor().dropTable(queryContext, dropTable.getName(), false,
            dropTable.getPurge());
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public FunctionListResponse getFunctionList(RpcController controller, SessionedStringProto request)
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
        return FunctionListResponse.newBuilder()
            .setState(OK)
            .addAllFunction(functionProtos)
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return FunctionListResponse.newBuilder().
            setState(returnError(t))
            .build();
      }
    }

    @Override
    public PartitionListResponse getPartitionsByTableName(RpcController controller, SessionedStringProto request)
      throws ServiceException {

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

        List<PartitionDescProto> partitions = catalog.getPartitions(databaseName, tableName);
        return PartitionListResponse.newBuilder()
          .setState(OK)
          .addAllPartition(partitions)
          .build();
      } catch (Throwable t) {
        return PartitionListResponse.newBuilder()
          .setState(returnError(t))
          .build();
      }
    }

    @Override
    public IndexResponse getIndexWithName(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        String indexName, databaseName;
        if (CatalogUtil.isFQTableName(request.getValue())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getValue());
          databaseName = splitted[0];
          indexName = splitted[1];
        } else {
          databaseName = session.getCurrentDatabase();
          indexName = request.getValue();
        }
        IndexDescProto indexProto = catalog.getIndexByName(databaseName, indexName).getProto();
        return IndexResponse.newBuilder()
            .setState(OK)
            .setIndexDesc(indexProto)
            .build();

      } catch (Throwable t) {
        return IndexResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public ReturnState existIndexWithName(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        String indexName, databaseName;
        if (CatalogUtil.isFQTableName(request.getValue())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getValue());
          databaseName = splitted[0];
          indexName = splitted[1];
        } else {
          databaseName = session.getCurrentDatabase();
          indexName = request.getValue();
        }

        if (catalog.existIndexByName(databaseName, indexName)) {
          return OK;
        } else {
          return errUndefinedIndexName(indexName);
        }
      } catch (Throwable t) {
        return returnError(t);
      }
    }

    @Override
    public IndexListResponse getIndexesForTable(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        String tableName, databaseName;
        if (CatalogUtil.isFQTableName(request.getValue())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getValue());
          databaseName = splitted[0];
          tableName = splitted[1];
        } else {
          databaseName = session.getCurrentDatabase();
          tableName = request.getValue();
        }

        IndexListResponse.Builder builder = IndexListResponse.newBuilder().setState(OK);
        for (IndexDesc index : catalog.getAllIndexesByTable(databaseName, tableName)) {
          builder.addIndexDesc(index.getProto());
        }
        return builder.build();
      } catch (Throwable t) {
        return IndexListResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public ReturnState existIndexesForTable(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        String tableName, databaseName;
        if (CatalogUtil.isFQTableName(request.getValue())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getValue());
          databaseName = splitted[0];
          tableName = splitted[1];
        } else {
          databaseName = session.getCurrentDatabase();
          tableName = request.getValue();
        }
        if (catalog.existIndexesByTable(databaseName, tableName)) {
          return OK;
        } else {
          return errUndefinedIndex(tableName);
        }
      } catch (Throwable t) {
        return returnError(t);
      }
    }

    @Override
    public IndexResponse getIndexWithColumns(RpcController controller, GetIndexWithColumnsRequest request)
        throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        String tableName, databaseName;
        if (CatalogUtil.isFQTableName(request.getTableName())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getTableName());
          databaseName = splitted[0];
          tableName = splitted[1];
        } else {
          databaseName = session.getCurrentDatabase();
          tableName = request.getTableName();
        }
        String[] columnNames = new String[request.getColumnNamesCount()];
        columnNames = request.getColumnNamesList().toArray(columnNames);

        return IndexResponse.newBuilder()
            .setState(OK)
            .setIndexDesc(catalog.getIndexByColumnNames(databaseName, tableName, columnNames).getProto())
            .build();

      } catch (Throwable t) {
        return IndexResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public ReturnState existIndexWithColumns(RpcController controller, GetIndexWithColumnsRequest request)
        throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());

        String tableName, databaseName;
        if (CatalogUtil.isFQTableName(request.getTableName())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getTableName());
          databaseName = splitted[0];
          tableName = splitted[1];
        } else {
          databaseName = session.getCurrentDatabase();
          tableName = request.getTableName();
        }
        String[] columnNames = new String[request.getColumnNamesCount()];
        columnNames = request.getColumnNamesList().toArray(columnNames);
        if (catalog.existIndexByColumnNames(databaseName, tableName, columnNames)) {
          return OK;
        } else {
          return errUndefinedIndex(tableName, request.getColumnNamesList());
        }
      } catch (Throwable t) {
        return returnError(t);
      }
    }

    @Override
    public ReturnState dropIndex(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      try {
        context.getSessionManager().touch(request.getSessionId().getId());
        Session session = context.getSessionManager().getSession(request.getSessionId().getId());
        QueryContext queryContext = new QueryContext(conf, session);

        String indexName, databaseName;
        if (CatalogUtil.isFQTableName(request.getValue())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getValue());
          databaseName = splitted[0];
          indexName = splitted[1];
        } else {
          databaseName = session.getCurrentDatabase();
          indexName = request.getValue();
        }
        catalog.dropIndex(databaseName, indexName);

        return OK;
      } catch (Throwable t) {
        return returnError(t);
      }
    }
  }
}
