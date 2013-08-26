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
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.ResultSetImpl;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.QueryMasterClientProtocol;
import org.apache.tajo.ipc.QueryMasterClientProtocol.QueryMasterClientProtocolService;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;
import org.apache.tajo.rpc.ProtoBlockingRpcClient;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import org.apache.tajo.util.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TajoClient is ThreadSafe
 */
public class TajoClient {
  private final Log LOG = LogFactory.getLog(TajoClient.class);

  private final TajoConf conf;
  private ProtoBlockingRpcClient tasjoMasterClient;
  private TajoMasterClientProtocolService.BlockingInterface tajoMasterService;

  private Map<QueryId, QueryMasterClientProtocolService.BlockingInterface> queryMasterConnectionMap =
          new HashMap<QueryId, QueryMasterClientProtocolService.BlockingInterface>();

  private Map<QueryId, ProtoBlockingRpcClient> queryMasterClientMap =
          new HashMap<QueryId, ProtoBlockingRpcClient>();

  public TajoClient(TajoConf conf) throws IOException {
    this.conf = conf;
    String masterAddr = this.conf.getVar(ConfVars.CLIENT_SERVICE_ADDRESS);
    InetSocketAddress addr = NetUtils.createSocketAddr(masterAddr);
    connect(addr);
  }

  public TajoClient(InetSocketAddress addr) throws IOException {
    this.conf = new TajoConf();
    connect(addr);
  }

  public TajoClient(String hostname, int port) throws IOException {
    this.conf = new TajoConf();
    connect(NetUtils.createSocketAddr(hostname, port));
  }

  private void connect(InetSocketAddress addr) throws IOException {
    try {
      tasjoMasterClient = new ProtoBlockingRpcClient(TajoMasterClientProtocol.class, addr);
      tajoMasterService = tasjoMasterClient.getStub();
    } catch (Exception e) {
      throw new IOException(e);
    }

    LOG.info("connected to tajo cluster (" +
        org.apache.tajo.util.NetUtils.normalizeInetSocketAddress(addr) + ")");
  }

  public void close() {
    tasjoMasterClient.close();

    for(ProtoBlockingRpcClient eachClient: queryMasterClientMap.values()) {
      eachClient.close();
    }
    queryMasterClientMap.clear();
    queryMasterConnectionMap.clear();
  }

  public void closeQuery(QueryId queryId) {
    if(queryMasterClientMap.containsKey(queryId)) {
      try {
        queryMasterConnectionMap.get(queryId).killQuery(null, queryId.getProto());
      } catch (Exception e) {
        LOG.warn("Fail to close a QueryMaster connection (qid=" + queryId + ", msg=" + e.getMessage() + ")", e);
      }
      queryMasterClientMap.get(queryId).close();
      LOG.info("Closed a QueryMaster connection (qid=" + queryId + ", addr="
          + queryMasterClientMap.get(queryId).getRemoteAddress() + ")");
      queryMasterClientMap.remove(queryId);
      queryMasterConnectionMap.remove(queryId);
    }
  }

  public boolean isConnected() {
    return tasjoMasterClient.isConnected();
  }

  /**
   * It submits a query statement and get a response immediately.
   * The response only contains a query id, and submission status.
   * In order to get the result, you should use {@link #getQueryResult(org.apache.tajo.QueryId)}
   * or {@link #getQueryResultAndWait(org.apache.tajo.QueryId)}.
   */
  public GetQueryStatusResponse executeQuery(String tql) throws ServiceException {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setQuery(tql);

    return tajoMasterService.submitQuery(null, builder.build());
  }

  /**
   * It submits a query statement and get a response.
   * The main difference from {@link #executeQuery(String)}
   * is a blocking method. So, this method is wait for
   * the finish of the submitted query.
   *
   * @return If failed, return null.
   */
  public ResultSet executeQueryAndGetResult(String sql)
      throws ServiceException, IOException {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setQuery(sql);
    GetQueryStatusResponse response = tajoMasterService.submitQuery(null, builder.build());
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
    if(queryMasterConnectionMap.containsKey(queryId)) {
      QueryMasterClientProtocolService.BlockingInterface queryMasterService = queryMasterConnectionMap.get(queryId);
      res = queryMasterService.getQueryStatus(null, builder.build());
    } else {
      res = tajoMasterService.getQueryStatus(null, builder.build());

      String queryMasterHost = res.getQueryMasterHost();
      if(queryMasterHost != null && !queryMasterHost.isEmpty()) {
        connectionToQueryMaster(queryId, queryMasterHost, res.getQueryMasterPort());

        QueryMasterClientProtocolService.BlockingInterface queryMasterService = queryMasterConnectionMap.get(queryId);
        res = queryMasterService.getQueryStatus(null, builder.build());
      }
    }
    return new QueryStatus(res);
  }

  private void connectionToQueryMaster(QueryId queryId, String queryMasterHost, int queryMasterPort) {
    try {
      InetSocketAddress addr = NetUtils.createSocketAddr(queryMasterHost, queryMasterPort);
      ProtoBlockingRpcClient client = new ProtoBlockingRpcClient(QueryMasterClientProtocol.class, addr);
      QueryMasterClientProtocolService.BlockingInterface service = client.getStub();

      queryMasterConnectionMap.put(queryId, service);
      queryMasterClientMap.put(queryId, client);

      LOG.info("Connected to Query Master (qid=" + queryId + ", addr=" +
          NetUtils.normalizeInetSocketAddress(addr) + ")");
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private static boolean isQueryRunnning(QueryState state) {
    return state == QueryState.QUERY_NEW ||
        state == QueryState.QUERY_INIT ||
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

    TableDesc tableDesc = getResultDesc(queryId);
    return new ResultSetImpl(this, queryId, conf, tableDesc.getPath());
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
    return new ResultSetImpl(this, queryId);
  }

  public TableDesc getResultDesc(QueryId queryId) throws ServiceException {
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return null;
    }

    QueryMasterClientProtocolService.BlockingInterface queryMasterService = queryMasterConnectionMap.get(queryId);
    if(queryMasterService == null) {
      LOG.warn("No Connection to QueryMaster for " + queryId);
      return null;
    }
    GetQueryResultRequest.Builder builder = GetQueryResultRequest.newBuilder();
    builder.setQueryId(queryId.getProto());
    GetQueryResultResponse response = queryMasterService.getQueryResult(null,
        builder.build());

    return CatalogUtil.newTableDesc(response.getTableDesc());
  }

  public boolean updateQuery(String tql) throws ServiceException {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setQuery(tql);

    ResultCode resultCode =
        tajoMasterService.updateQuery(null, builder.build()).getResultCode();
    return resultCode == ResultCode.OK;
  }

  public boolean existTable(String name) throws ServiceException {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    return tajoMasterService.existTable(null, builder.build()).getValue();
  }

  public TableDesc attachTable(String name, String path)
      throws ServiceException {
    AttachTableRequest.Builder builder = AttachTableRequest.newBuilder();
    builder.setName(name);
    builder.setPath(path);
    TableResponse res = tajoMasterService.attachTable(null, builder.build());
    return CatalogUtil.newTableDesc(res.getTableDesc());
  }

  public TableDesc attachTable(String name, Path path)
      throws ServiceException {
    return attachTable(name, path.toString());
  }

  public boolean detachTable(String name) throws ServiceException {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    return tajoMasterService.detachTable(null, builder.build()).getValue();
  }

  public TableDesc createTable(String name, Path path, TableMeta meta)
      throws ServiceException {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setName(name);
    builder.setPath(path.toString());
    builder.setMeta(meta.getProto());
    TableResponse res = tajoMasterService.createTable(null, builder.build());
    return CatalogUtil.newTableDesc(res.getTableDesc());
  }

  public boolean dropTable(String name) throws ServiceException {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    return tajoMasterService.dropTable(null, builder.build()).getValue();
  }

  /**
   * Get a list of table names. All table and column names are
   * represented as lower-case letters.
   */
  public List<String> getTableList() throws ServiceException {
    GetTableListRequest.Builder builder = GetTableListRequest.newBuilder();
    GetTableListResponse res = tajoMasterService.getTableList(null, builder.build());
    return res.getTablesList();
  }

  public TableDesc getTableDesc(String tableName) throws ServiceException {
    GetTableDescRequest.Builder build = GetTableDescRequest.newBuilder();
    build.setTableName(tableName);
    TableResponse res = tajoMasterService.getTableDesc(null, build.build());
    if (res == null) {
      return null;
    } else {
      return CatalogUtil.newTableDesc(res.getTableDesc());
    }
  }

  public boolean killQuery(QueryId queryId)
      throws ServiceException, IOException {

    QueryStatus status = getQueryStatus(queryId);

    try {
      /* send a kill to the TM */
      tajoMasterService.killQuery(null, queryId.getProto());
      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      while ((currentTimeMillis < timeKillIssued + 10000L) && (status.getState()
          != QueryState.QUERY_KILLED)) {
        try {
          Thread.sleep(1000L);
        } catch(InterruptedException ie) {
          /** interrupted, just break */
          break;
        }
        currentTimeMillis = System.currentTimeMillis();
        status = getQueryStatus(queryId);
      }
    } catch(ServiceException io) {
      LOG.debug("Error when checking for application status", io);
      return false;
    }

    return true;
  }

  public static void main(String[] args) throws Exception {
    TajoClient client = new TajoClient(new TajoConf());

    client.close();

    synchronized(client) {
      client.wait();
    }
  }
}