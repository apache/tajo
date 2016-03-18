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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.ResourceProtos.AllocationResourceProto;
import org.apache.tajo.ResourceProtos.QueryExecutionRequest;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.rm.TajoResourceManager;
import org.apache.tajo.plan.logical.LogicalRootNode;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.session.Session;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.RpcParameterFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueryInProgress {
  private static final Log LOG = LogFactory.getLog(QueryInProgress.class.getName());

  private QueryId queryId;

  private Session session;

  private LogicalRootNode plan;

  private volatile boolean querySubmitted;

  private volatile boolean isStopped;

  private QueryInfo queryInfo;

  private final TajoMaster.MasterContext masterContext;

  private NettyClientBase queryMasterRpc;

  private QueryMasterProtocolService queryMasterRpcClient;

  private AllocationResourceProto allocationResource;

  private final Properties rpcParams;

  private final Lock readLock;
  private final Lock writeLock;

  public QueryInProgress(
      TajoMaster.MasterContext masterContext,
      Session session,
      QueryContext queryContext,
      QueryId queryId, String sql, String jsonExpr, LogicalRootNode plan) {

    this.masterContext = masterContext;
    this.session = session;
    this.queryId = queryId;
    this.plan = plan;

    queryInfo = new QueryInfo(queryId, queryContext, sql, jsonExpr);
    queryInfo.setStartTime(System.currentTimeMillis());

    rpcParams = RpcParameterFactory.get(masterContext.getConf());

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
  }

  public void kill() {
    writeLock.lock();
    try {
      getQueryInfo().setQueryState(TajoProtos.QueryState.QUERY_KILLED);
      if (queryMasterRpcClient != null) {
        CallFuture<PrimitiveProtos.NullProto> callFuture = new CallFuture<>();
        queryMasterRpcClient.killQuery(callFuture.getController(), queryId.getProto(), callFuture);
        callFuture.get();
      }
    } catch (Throwable e) {
      catchException("Failed to kill query " + queryId + " by exception " + e, e);
    } finally {
      writeLock.unlock();
    }
  }

  public void stopProgress() {
    if (isStopped) {
      return;
    } else {
      isStopped = true;
    }

    LOG.info("=========================================================");
    LOG.info("Stop query:" + queryId);

    masterContext.getResourceManager().getScheduler().stopQuery(queryId);

    RpcClientManager.cleanup(queryMasterRpc);

    try {
      masterContext.getHistoryWriter().appendAndFlush(queryInfo);
    } catch (Throwable e) {
      LOG.warn(e, e);
    }
  }

  /**
   * Connect to QueryMaster and allocate QM resource.
   *
   * @param allocation QM resource
   * @return If there is no available resource, It returns false
   */
  protected boolean allocateToQueryMaster(AllocationResourceProto allocation) {
    try {
      writeLock.lockInterruptibly();
    } catch (Exception e) {
      catchException("Failed to lock by exception " + e, e);
      return false;
    }
    try {
      TajoResourceManager resourceManager = masterContext.getResourceManager();
      WorkerConnectionInfo connectionInfo =
          resourceManager.getRMContext().getNodes().get(allocation.getWorkerId()).getConnectionInfo();
      try {
        if(queryMasterRpcClient == null) {
          connectQueryMaster(connectionInfo);
        }

        CallFuture<PrimitiveProtos.BoolProto> callFuture = new CallFuture<>();
        queryMasterRpcClient.allocateQueryMaster(callFuture.getController(), allocation, callFuture);

        if(!callFuture.get().getValue()) return false;

      } catch (ConnectException ce) {
        return false;
      }

      LOG.info("Initializing QueryInProgress for QueryID=" + queryId);
      this.allocationResource = allocation;
      this.queryInfo.setQueryMaster(connectionInfo.getHost());
      this.queryInfo.setQueryMasterPort(connectionInfo.getQueryMasterPort());
      this.queryInfo.setQueryMasterclientPort(connectionInfo.getClientPort());
      this.queryInfo.setQueryMasterInfoPort(connectionInfo.getHttpInfoPort());

      return true;
    } catch (Exception e) {
      catchException("Failed to start query master for query " + queryId + " by exception " + e, e);
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  private void connectQueryMaster(WorkerConnectionInfo connectionInfo)
      throws NoSuchMethodException, ConnectException, ClassNotFoundException {
    RpcClientManager.cleanup(queryMasterRpc);

    InetSocketAddress addr = NetUtils.createSocketAddr(connectionInfo.getHost(), connectionInfo.getQueryMasterPort());
    LOG.info("Try to connect to QueryMaster:" + addr);
    queryMasterRpc = RpcClientManager.getInstance().newClient(addr, QueryMasterProtocol.class, true, rpcParams);
    queryMasterRpcClient = queryMasterRpc.getStub();
  }

  /**
   * Launch the allocated query to QueryMaster
   */
  public boolean submitToQueryMaster() {
    if(querySubmitted) {
      return false;
    }

    try {
      writeLock.lockInterruptibly();
    } catch (Exception e) {
      LOG.error("Failed to lock by exception " + e.getMessage(), e);
      return false;
    }

    try {

      LOG.info("Call executeQuery to :" +
          queryInfo.getQueryMasterHost() + ":" + queryInfo.getQueryMasterPort() + "," + queryId);

      QueryExecutionRequest.Builder builder = QueryExecutionRequest.newBuilder();
      builder.setQueryId(queryId.getProto())
          .setQueryContext(queryInfo.getQueryContext().getProto())
          .setSession(session.getProto())
          .setExprInJson(PrimitiveProtos.StringProto.newBuilder().setValue(queryInfo.getJsonExpr()))
          .setLogicalPlanJson(PrimitiveProtos.StringProto.newBuilder().setValue(plan.toJson()).build())
          .setAllocation(allocationResource);

      CallFuture<PrimitiveProtos.NullProto> callFuture = new CallFuture<>();
      queryMasterRpcClient.executeQuery(callFuture.getController(), builder.build(), callFuture);
      callFuture.get();

      querySubmitted = true;
      getQueryInfo().setQueryState(TajoProtos.QueryState.QUERY_MASTER_LAUNCHED);
      return true;
    } catch (Exception e) {
      LOG.error("Failed to submit query " + queryId + " to master by exception " + e, e);
      catchException(e.getMessage(), e);
    } finally {
      writeLock.unlock();
    }
    return false;
  }

  public void catchException(String message, Throwable e) {
    LOG.error(message, e);
    queryInfo.setQueryState(TajoProtos.QueryState.QUERY_FAILED);
    queryInfo.setLastMessage(StringUtils.stringifyException(e));
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public QueryInfo getQueryInfo() {
    readLock.lock();
    try {
      return this.queryInfo;
    } finally {
      readLock.unlock();
    }
  }

  public void heartbeat(QueryInfo queryInfo) {
    LOG.info("Received QueryMaster heartbeat:" + queryInfo);

    writeLock.lock();
    try {
      this.queryInfo.setQueryState(queryInfo.getQueryState());
      this.queryInfo.setProgress(queryInfo.getProgress());

      // Update diagnosis message
      if (queryInfo.getLastMessage() != null && !queryInfo.getLastMessage().isEmpty()) {
        this.queryInfo.setLastMessage(queryInfo.getLastMessage());
      }

      // if any error occurs, print outs the error message
      if (this.queryInfo.getQueryState() == QueryState.QUERY_FAILED ||
          this.queryInfo.getQueryState() == QueryState.QUERY_ERROR) {
        LOG.error(queryId + " is stopped because " + queryInfo.getLastMessage());
      }

      // terminal state will let client to retrieve a query result
      // So, we must set the query result before changing query state
      if (isFinishState()) {
        if (queryInfo.hasResultdesc()) {
          this.queryInfo.setResultDesc(queryInfo.getResultDesc());
        }

        this.queryInfo.setFinishTime(System.currentTimeMillis());
        masterContext.getQueryJobManager().stopQuery(queryInfo.getQueryId());
      }
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isKillWait() {
    TajoProtos.QueryState state = queryInfo.getQueryState();
    return state == TajoProtos.QueryState.QUERY_KILL_WAIT;
  }

  public boolean isFinishState() {
    TajoProtos.QueryState state = queryInfo.getQueryState();
    return state == TajoProtos.QueryState.QUERY_FAILED ||
        state == TajoProtos.QueryState.QUERY_ERROR ||
        state == TajoProtos.QueryState.QUERY_KILLED ||
        state == TajoProtos.QueryState.QUERY_SUCCEEDED;
  }
}
