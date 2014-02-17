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

package org.apache.tajo.client;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.QueryMasterClientProtocol;
import org.apache.tajo.ipc.QueryMasterClientProtocol.QueryMasterClientProtocolService;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;
import org.apache.tajo.jdbc.SQLStates;
import org.apache.tajo.jdbc.TajoResultSet;
import org.apache.tajo.rpc.*;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.rpc.ServerCallable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class TajoClient {
  private final Log LOG = LogFactory.getLog(TajoClient.class);

  private final TajoConf conf;

  private Map<QueryId, InetSocketAddress> queryMasterMap = new ConcurrentHashMap<QueryId, InetSocketAddress>();

  private InetSocketAddress tajoMasterAddr;

  private RpcConnectionPool connPool;

  public TajoClient(TajoConf conf) throws IOException {
    this(conf, NetUtils.createSocketAddr(conf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS)));
  }

  public TajoClient(TajoConf conf, InetSocketAddress addr) throws IOException {
    this.conf = conf;
    this.conf.set("tajo.disk.scheduler.report.interval", "0");
    this.tajoMasterAddr = addr;
    int workerNum = conf.getIntVar(TajoConf.ConfVars.RPC_CLIENT_WORKER_THREAD_NUM);
    //Don't share connection pool per client
    connPool = RpcConnectionPool.newPool(conf, getClass().getSimpleName(), workerNum);
  }

  public TajoClient(InetSocketAddress addr) throws IOException {
    this(new TajoConf(), addr);
  }

  public TajoClient(String hostname, int port) throws IOException {
    this(new TajoConf(), NetUtils.createSocketAddr(hostname, port));
  }

  public void close() {
    if(connPool != null) {
      connPool.shutdown();
    }
    queryMasterMap.clear();
  }

  public TajoConf getConf() {
    return conf;
  }

  /**
   * Call to QueryMaster closing query resources
   * @param queryId
   */
  public void closeQuery(final QueryId queryId) {
    if(queryMasterMap.containsKey(queryId)) {
      NettyClientBase qmClient = null;
      try {
        qmClient = connPool.getConnection(queryMasterMap.get(queryId), QueryMasterClientProtocol.class, false);
        QueryMasterClientProtocolService.BlockingInterface queryMasterService = qmClient.getStub();
        queryMasterService.closeQuery(null, queryId.getProto());
      } catch (Exception e) {
        LOG.warn("Fail to close a QueryMaster connection (qid=" + queryId + ", msg=" + e.getMessage() + ")", e);
      } finally {
        connPool.closeConnection(qmClient);
        queryMasterMap.remove(queryId);
      }
    }
  }

  public ExplainQueryResponse explainQuery(final String sql) throws ServiceException {
    return new ServerCallable<ExplainQueryResponse>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public ExplainQueryResponse call(NettyClientBase client) throws ServiceException {
        final ExplainQueryRequest.Builder builder = ExplainQueryRequest.newBuilder();
        builder.setQuery(sql);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.explainQuery(null, builder.build());
      }
    }.withRetries();
  }

  /**
   * It submits a query statement and get a response immediately.
   * The response only contains a query id, and submission status.
   * In order to get the result, you should use {@link #getQueryResult(org.apache.tajo.QueryId)}
   * or {@link #getQueryResultAndWait(org.apache.tajo.QueryId)}.
   */
  public GetQueryStatusResponse executeQuery(final String sql) throws ServiceException {
    return new ServerCallable<GetQueryStatusResponse>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public GetQueryStatusResponse call(NettyClientBase client) throws ServiceException {
        final QueryRequest.Builder builder = QueryRequest.newBuilder();
        builder.setQuery(sql);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.submitQuery(null, builder.build());
      }
    }.withRetries();
  }

  /**
   * It submits a query statement and get a response.
   * The main difference from {@link #executeQuery(String)}
   * is a blocking method. So, this method is wait for
   * the finish of the submitted query.
   *
   * @return If failed, return null.
   */
  public ResultSet executeQueryAndGetResult(final String sql)
      throws ServiceException, IOException {
    GetQueryStatusResponse response = new ServerCallable<GetQueryStatusResponse>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public GetQueryStatusResponse call(NettyClientBase client) throws ServiceException {
        final QueryRequest.Builder builder = QueryRequest.newBuilder();
        builder.setQuery(sql);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.submitQuery(null, builder.build());
      }
    }.withRetries();

    QueryId queryId = new QueryId(response.getQueryId());
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return this.createNullResultSet(queryId);
    } else {
      return this.getQueryResultAndWait(queryId);
    }
  }

  public QueryStatus getQueryStatus(QueryId queryId) throws ServiceException {
    GetQueryStatusRequest.Builder builder
        = GetQueryStatusRequest.newBuilder();
    builder.setQueryId(queryId.getProto());

    GetQueryStatusResponse res = null;
    if(queryMasterMap.containsKey(queryId)) {
      NettyClientBase qmClient = null;
      try {
        qmClient = connPool.getConnection(queryMasterMap.get(queryId),
            QueryMasterClientProtocol.class, false);
        QueryMasterClientProtocolService.BlockingInterface queryMasterService = qmClient.getStub();
        res = queryMasterService.getQueryStatus(null, builder.build());
      } catch (Exception e) {
        throw new ServiceException(e.getMessage(), e);
      } finally {
        connPool.releaseConnection(qmClient);
      }
    } else {
      NettyClientBase tmClient = null;
      try {
        tmClient = connPool.getConnection(tajoMasterAddr,
            TajoMasterClientProtocol.class, false);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = tmClient.getStub();
        res = tajoMasterService.getQueryStatus(null, builder.build());

        String queryMasterHost = res.getQueryMasterHost();
        if(queryMasterHost != null && !queryMasterHost.isEmpty()) {
          NettyClientBase qmClient = null;
          try {
            InetSocketAddress qmAddr = NetUtils.createSocketAddr(queryMasterHost, res.getQueryMasterPort());
            qmClient = connPool.getConnection(
                qmAddr, QueryMasterClientProtocol.class, false);
            QueryMasterClientProtocolService.BlockingInterface queryMasterService = qmClient.getStub();
            res = queryMasterService.getQueryStatus(null, builder.build());

            queryMasterMap.put(queryId, qmAddr);
          } catch (Exception e) {
            throw new ServiceException(e.getMessage(), e);
          } finally {
            connPool.releaseConnection(qmClient);
          }
        }
      } catch (Exception e) {
        throw new ServiceException(e.getMessage(), e);
      } finally {
        connPool.releaseConnection(tmClient);
      }
    }
    return new QueryStatus(res);
  }

  public static boolean isQueryRunnning(QueryState state) {
    return state == QueryState.QUERY_NEW ||
        state == QueryState.QUERY_RUNNING ||
        state == QueryState.QUERY_MASTER_LAUNCHED ||
        state == QueryState.QUERY_MASTER_INIT ||
        state == QueryState.QUERY_NOT_ASSIGNED;
  }

  public ResultSet getQueryResult(QueryId queryId)
      throws ServiceException, IOException {
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return createNullResultSet(queryId);
    }
    GetQueryResultResponse response = getResultResponse(queryId);
    TableDesc tableDesc = CatalogUtil.newTableDesc(response.getTableDesc());
    conf.setVar(ConfVars.USERNAME, response.getTajoUserName());
    return new TajoResultSet(this, queryId, conf, tableDesc);
  }

  public ResultSet getQueryResultAndWait(QueryId queryId)
      throws ServiceException, IOException {
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return createNullResultSet(queryId);
    }
    QueryStatus status = getQueryStatus(queryId);

    while(status != null && isQueryRunnning(status.getState())) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      status = getQueryStatus(queryId);
    }

    if (status.getState() == QueryState.QUERY_SUCCEEDED) {
      if (status.hasResult()) {
        return getQueryResult(queryId);
      } else {
        return createNullResultSet(queryId);
      }

    } else {
      LOG.warn("Query (" + status.getQueryId() + ") failed: " + status.getState());

      //TODO throw SQLException(?)
      return createNullResultSet(queryId);
    }
  }

  public ResultSet createNullResultSet(QueryId queryId) throws IOException {
    return new TajoResultSet(this, queryId);
  }

  public GetQueryResultResponse getResultResponse(QueryId queryId) throws ServiceException {
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return null;
    }

    NettyClientBase client = null;
    try {
      InetSocketAddress queryMasterAddr = queryMasterMap.get(queryId);
      if(queryMasterAddr == null) {
        LOG.warn("No Connection to QueryMaster for " + queryId);
        return null;
      }
      client = connPool.getConnection(queryMasterAddr, QueryMasterClientProtocol.class, false);
      QueryMasterClientProtocolService.BlockingInterface queryMasterService = client.getStub();
      GetQueryResultRequest.Builder builder = GetQueryResultRequest.newBuilder();
      builder.setQueryId(queryId.getProto());
      GetQueryResultResponse response = queryMasterService.getQueryResult(null,
          builder.build());

      return response;
    } catch (Exception e) {
      throw new ServiceException(e.getMessage(), e);
    } finally {
      connPool.releaseConnection(client);
    }
  }

  public boolean updateQuery(final String sql) throws ServiceException {
    return new ServerCallable<Boolean>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        QueryRequest.Builder builder = QueryRequest.newBuilder();
        builder.setQuery(sql);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        UpdateQueryResponse response = tajoMasterService.updateQuery(null, builder.build());
        if (response.getResultCode() == ResultCode.OK) {
          return true;
        } else {
          if (response.hasErrorMessage()) {
            LOG.error(response.getErrorMessage());
          }
          return false;
        }
      }
    }.withRetries();
  }

  /**
   * Test for the existence of table in catalog data.
   * <p/>
   * This will return true if table exists, false if not.
   * @param name
   * @return
   * @throws ServiceException
   */
  public boolean existTable(final String name) throws ServiceException {
    return new ServerCallable<Boolean>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        StringProto.Builder builder = StringProto.newBuilder();
        builder.setValue(name);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.existTable(null, builder.build()).getValue();
      }
    }.withRetries();
  }

  public TableDesc createExternalTable(final String name, final Schema schema, final Path path, final TableMeta meta)
      throws SQLException, ServiceException {
    return new ServerCallable<TableDesc>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public TableDesc call(NettyClientBase client) throws ServiceException, SQLException {
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
        builder.setName(name);
        builder.setSchema(schema.getProto());
        builder.setMeta(meta.getProto());
        builder.setPath(path.toUri().toString());
        TableResponse res = tajoMasterService.createExternalTable(null, builder.build());
        if (res.getResultCode() == ResultCode.OK) {
          return CatalogUtil.newTableDesc(res.getTableDesc());
        } else {
          throw new SQLException(res.getErrorMessage(), SQLStates.ER_NO_SUCH_TABLE.getState());
        }
      }
    }.withRetries();
  }

  public boolean dropTable(final String tableName) throws ServiceException {
    return dropTable(tableName, false);
  }

  /**
   * Deletes table schema from catalog data and deletes data file in hdfs
   * @param tableName
   * @return
   * @throws ServiceException
   */
  public boolean dropTable(final String tableName, final boolean purge) throws ServiceException {
    return new ServerCallable<Boolean>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        DropTableRequest.Builder builder = DropTableRequest.newBuilder();
        builder.setName(tableName);
        builder.setPurge(purge);
        return tajoMasterService.dropTable(null, builder.build()).getValue();
      }
    }.withRetries();

  }

  public List<BriefQueryInfo> getQueryList() throws ServiceException {
    return new ServerCallable<List<BriefQueryInfo>>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public List<BriefQueryInfo> call(NettyClientBase client) throws ServiceException {
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        GetQueryListRequest.Builder builder = GetQueryListRequest.newBuilder();
        GetQueryListResponse res = tajoMasterService.getQueryList(null, builder.build());
        return res.getQueryListList();
      }
    }.withRetries();
  }

  public List<WorkerResourceInfo> getClusterInfo() throws ServiceException {
    return new ServerCallable<List<WorkerResourceInfo>>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public List<WorkerResourceInfo> call(NettyClientBase client) throws ServiceException {
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        GetClusterInfoRequest.Builder builder = GetClusterInfoRequest.newBuilder();
        GetClusterInfoResponse res = tajoMasterService.getClusterInfo(null, builder.build());
        return res.getWorkerListList();
      }
    }.withRetries();
  }

  /**
   * Get a list of table names. All table and column names are
   * represented as lower-case letters.
   */
  public List<String> getTableList() throws ServiceException {
    return new ServerCallable<List<String>>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public List<String> call(NettyClientBase client) throws ServiceException {
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        GetTableListRequest.Builder builder = GetTableListRequest.newBuilder();
        GetTableListResponse res = tajoMasterService.getTableList(null, builder.build());
        return res.getTablesList();
      }
    }.withRetries();
  }

  public TableDesc getTableDesc(final String tableName) throws SQLException, ServiceException {
    return new ServerCallable<TableDesc>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public TableDesc call(NettyClientBase client) throws ServiceException, SQLException {
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        GetTableDescRequest.Builder build = GetTableDescRequest.newBuilder();
        build.setTableName(tableName);
        TableResponse res = tajoMasterService.getTableDesc(null, build.build());
        if (res.getResultCode() == ResultCode.OK) {
          return CatalogUtil.newTableDesc(res.getTableDesc());
        } else {
          throw new SQLException(res.getErrorMessage(), SQLStates.ER_NO_SUCH_TABLE.getState());
        }
      }
    }.withRetries();
  }

  public boolean killQuery(final QueryId queryId)
      throws ServiceException, IOException {

    QueryStatus status = getQueryStatus(queryId);

    NettyClientBase tmClient = null;
    try {
      /* send a kill to the TM */
      tmClient = connPool.getConnection(tajoMasterAddr, TajoMasterClientProtocol.class, false);
      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = tmClient.getStub();
      tajoMasterService.killQuery(null, queryId.getProto());

      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      while ((currentTimeMillis < timeKillIssued + 10000L) && (status.getState() != QueryState.QUERY_KILLED)) {
        try {
          Thread.sleep(100L);
        } catch(InterruptedException ie) {
          break;
        }
        currentTimeMillis = System.currentTimeMillis();
        status = getQueryStatus(queryId);
      }
      return status.getState() == QueryState.QUERY_KILLED;
    } catch(Exception e) {
      LOG.debug("Error when checking for application status", e);
      return false;
    } finally {
      connPool.releaseConnection(tmClient);
    }
  }

  public List<CatalogProtos.FunctionDescProto> getFunctions(final String functionName) throws ServiceException {
    return new ServerCallable<List<CatalogProtos.FunctionDescProto>>(conf, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public List<CatalogProtos.FunctionDescProto> call(NettyClientBase client) throws ServiceException, SQLException {
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        String paramFunctionName = functionName == null ? "" : functionName;

        FunctionResponse res = tajoMasterService.getFunctionList(null,
            StringProto.newBuilder().setValue(paramFunctionName).build());
        if (res.getResultCode() == ResultCode.OK) {
          return res.getFunctionsList();
        } else {
          throw new SQLException(res.getErrorMessage());
        }
      }
    }.withRetries();
  }
}
