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
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.WorkerAllocatedResource;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol.QueryExecutionRequestProto;
import org.apache.tajo.master.rm.WorkerResourceManager;
import org.apache.tajo.plan.logical.LogicalRootNode;
import org.apache.tajo.rpc.*;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.session.Session;
import org.apache.tajo.util.NetUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueryInProgress {
  private static final Log LOG = LogFactory.getLog(QueryInProgress.class.getName());

  private QueryId queryId;

  private Session session;

  private LogicalRootNode plan;

  private AtomicBoolean querySubmitted = new AtomicBoolean(false);

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private QueryInfo queryInfo;

  private final TajoMaster.MasterContext masterContext;

  private NettyClientBase queryMasterRpc;

  private QueryMasterProtocolService queryMasterRpcClient;

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

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
  }

  public void kill() {
    writeLock.lock();
    try {
      getQueryInfo().setQueryState(TajoProtos.QueryState.QUERY_KILLED);
      if (queryMasterRpcClient != null) {
        CallFuture<PrimitiveProtos.NullProto> callFuture = new CallFuture<PrimitiveProtos.NullProto>();
        queryMasterRpcClient.killQuery(callFuture.getController(), queryId.getProto(), callFuture);
        callFuture.get(RpcConstants.DEFAULT_FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    } catch (Throwable e) {
      catchException("Failed to kill query " + queryId + " by exception " + e, e);
    } finally {
      writeLock.unlock();
    }
  }

  public void stopProgress() {
    if(stopped.getAndSet(true)) {
      return;
    }

    LOG.info("=========================================================");
    LOG.info("Stop query:" + queryId);

    masterContext.getResourceManager().releaseQueryMaster(queryId);

    RpcClientManager.cleanup(queryMasterRpc);

    try {
      masterContext.getHistoryWriter().appendAndFlush(queryInfo);
    } catch (Throwable e) {
      LOG.warn(e, e);
    }
  }

  public boolean startQueryMaster() {
    try {
      writeLock.lockInterruptibly();
    } catch (Exception e) {
      catchException("Failed to lock by exception " + e, e);
      return false;
    }
    try {
      LOG.info("Initializing QueryInProgress for QueryID=" + queryId);
      WorkerResourceManager resourceManager = masterContext.getResourceManager();
      WorkerAllocatedResource resource = resourceManager.allocateQueryMaster(this);

      // if no resource to allocate a query master
      if(resource == null) {
        throw new RuntimeException("No Available Resources for QueryMaster");
      }

      queryInfo.setQueryMaster(resource.getConnectionInfo().getHost());
      queryInfo.setQueryMasterPort(resource.getConnectionInfo().getQueryMasterPort());
      queryInfo.setQueryMasterclientPort(resource.getConnectionInfo().getClientPort());
      queryInfo.setQueryMasterInfoPort(resource.getConnectionInfo().getHttpInfoPort());

      return true;
    } catch (Exception e) {
      catchException("Failed to start query master for query " + queryId + " by exception " + e, e);
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  private void connectQueryMaster() throws Exception {
    InetSocketAddress addr = NetUtils.createSocketAddr(queryInfo.getQueryMasterHost(), queryInfo.getQueryMasterPort());
    LOG.info("Connect to QueryMaster:" + addr);

    RpcClientManager.cleanup(queryMasterRpc);
    queryMasterRpc = RpcClientManager.getInstance().newClient(addr, QueryMasterProtocol.class, true);
    queryMasterRpcClient = queryMasterRpc.getStub();
  }

  public void submitQueryToMaster() {
    if(querySubmitted.get()) {
      return;
    }

    try {
      writeLock.lockInterruptibly();
    } catch (Exception e) {
      LOG.error("Failed to lock by exception " + e.getMessage(), e);
      return;
    }

    try {
      if(queryMasterRpcClient == null) {
        connectQueryMaster();
      }

      LOG.info("Call executeQuery to :" +
          queryInfo.getQueryMasterHost() + ":" + queryInfo.getQueryMasterPort() + "," + queryId);

      QueryExecutionRequestProto.Builder builder = TajoWorkerProtocol.QueryExecutionRequestProto.newBuilder();
      builder.setQueryId(queryId.getProto())
          .setQueryContext(queryInfo.getQueryContext().getProto())
          .setSession(session.getProto())
          .setExprInJson(PrimitiveProtos.StringProto.newBuilder().setValue(queryInfo.getJsonExpr()))
          .setLogicalPlanJson(PrimitiveProtos.StringProto.newBuilder().setValue(plan.toJson()).build());

      CallFuture<PrimitiveProtos.NullProto> callFuture = new CallFuture<PrimitiveProtos.NullProto>();
      queryMasterRpcClient.executeQuery(callFuture.getController(), builder.build(), callFuture);
      callFuture.get(RpcConstants.DEFAULT_FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      querySubmitted.set(true);
      getQueryInfo().setQueryState(TajoProtos.QueryState.QUERY_MASTER_LAUNCHED);
    } catch (Exception e) {
      LOG.error("Failed to submit query " + queryId + " to master by exception " + e, e);
      catchException(e.getMessage(), e);
    } finally {
      writeLock.unlock();
    }
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

  public boolean isStarted() {
    return !stopped.get() && this.querySubmitted.get();
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
        LOG.info(queryId + queryInfo.getLastMessage());
      }

      // if any error occurs, print outs the error message
      if (this.queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_FAILED) {
        LOG.warn(queryId + " failed, " + queryInfo.getLastMessage());
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
