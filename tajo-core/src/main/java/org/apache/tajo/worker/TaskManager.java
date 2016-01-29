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

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.rpc.AsyncRpcClient;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.RpcParameterFactory;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.event.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.tajo.ResourceProtos.*;

/**
 * A TaskManager is responsible for managing executionBlock resource and tasks.
 * */
public class TaskManager extends AbstractService implements EventHandler<TaskManagerEvent> {
  private static final Log LOG = LogFactory.getLog(TaskManager.class);

  private final TajoWorker.WorkerContext workerContext;
  private final Map<ExecutionBlockId, ExecutionBlockContext> executionBlockContextMap;
  private final Dispatcher dispatcher;
  private TaskExecutor executor;
  private final Properties rpcParams;
  private final TajoPullServerService pullServerService;

  public TaskManager(Dispatcher dispatcher, TajoWorker.WorkerContext workerContext) {
    this(dispatcher, workerContext, null, null);
  }

  public TaskManager(Dispatcher dispatcher, TajoWorker.WorkerContext workerContext,
                     TajoPullServerService pullServerService) {
    this(dispatcher, workerContext, null, pullServerService);
  }

  public TaskManager(Dispatcher dispatcher, TajoWorker.WorkerContext workerContext, TaskExecutor executor,
                     TajoPullServerService pullServerService) {
    super(TaskManager.class.getName());

    this.dispatcher = dispatcher;
    this.workerContext = workerContext;
    this.executionBlockContextMap = Maps.newHashMap();
    this.executor = executor;
    this.rpcParams = RpcParameterFactory.get(this.workerContext.getConf());
    this.pullServerService = pullServerService;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    dispatcher.register(TaskManagerEvent.EventType.class, this);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {

    executionBlockContextMap.values().parallelStream().forEach(ExecutionBlockContext::stop);
    super.serviceStop();
  }

  protected Dispatcher getDispatcher() {
    return dispatcher;
  }

  protected TajoWorker.WorkerContext getWorkerContext() {
    return workerContext;
  }

  protected TaskExecutor getTaskExecutor() {
    if (executor == null) {
      executor = workerContext.getTaskExecuor();
    }
    return executor;
  }

  public int getRunningTasks() {
    return workerContext.getTaskExecuor().getRunningTasks();
  }

  protected ExecutionBlockContext createExecutionBlock(ExecutionBlockId executionBlockId,
                                                       String queryMasterHostAndPort) {

    LOG.info("QueryMaster Address:" + queryMasterHostAndPort);

    AsyncRpcClient client = null;
    try {
      InetSocketAddress address = NetUtils.createSocketAddr(queryMasterHostAndPort);
      ExecutionBlockContextRequest.Builder request = ExecutionBlockContextRequest.newBuilder();
      request.setExecutionBlockId(executionBlockId.getProto())
          .setWorker(getWorkerContext().getConnectionInfo().getProto());

      client = RpcClientManager.getInstance().newClient(address, QueryMasterProtocol.class, true, rpcParams);
      QueryMasterProtocol.QueryMasterProtocolService.Interface stub = client.getStub();
      CallFuture<ExecutionBlockContextResponse> callback = new CallFuture<>();
      stub.getExecutionBlockContext(callback.getController(), request.build(), callback);

      ExecutionBlockContextResponse contextProto = callback.get();
      ExecutionBlockContext context = new ExecutionBlockContext(getWorkerContext(), contextProto, client,
          pullServerService);

      context.init();
      return context;
    } catch (Throwable e) {
      RpcClientManager.cleanup(client);
      LOG.fatal(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  protected void stopExecutionBlock(ExecutionBlockContext context,
                                    ExecutionBlockListProto cleanupList) {

    if (context != null) {
      try {
        context.getSharedResource().releaseBroadcastCache(context.getExecutionBlockId());
        context.sendShuffleReport();
        getWorkerContext().getTaskHistoryWriter().flushTaskHistories();
      } catch (Exception e) {
        LOG.fatal(e.getMessage(), e);
        throw new RuntimeException(e);
      } finally {
        context.stop();

          /* cleanup intermediate files */
        for (TajoIdProtos.ExecutionBlockIdProto ebId : cleanupList.getExecutionBlockIdList()) {
          String inputDir = ExecutionBlockContext.getBaseInputDir(new ExecutionBlockId(ebId)).toString();
          workerContext.cleanup(inputDir);
          String outputDir = ExecutionBlockContext.getBaseOutputDir(new ExecutionBlockId(ebId)).toString();
          workerContext.cleanup(outputDir);
        }
      }
      LOG.info("Stopped execution block:" + context.getExecutionBlockId());
    }
  }

  @Override
  public void handle(TaskManagerEvent event) {

    if(LOG.isDebugEnabled()) {
      LOG.debug("======================== Processing " + event + " of type " + event.getType());
    }

    switch (event.getType()) {
      case TASK_START: {
        //receive event from NodeResourceManager
        TaskStartEvent taskStartEvent = TUtil.checkTypeAndGet(event, TaskStartEvent.class);
        try {
          if (!executionBlockContextMap.containsKey(taskStartEvent.getExecutionBlockId())) {
            ExecutionBlockContext context = createExecutionBlock(taskStartEvent.getExecutionBlockId(),
                taskStartEvent.getTaskRequest().getQueryMasterHostAndPort());

            executionBlockContextMap.put(context.getExecutionBlockId(), context);
            LOG.info("Running ExecutionBlocks: " + executionBlockContextMap.size()
                + ", running tasks:" + getRunningTasks() + ", availableResource: "
                + workerContext.getNodeResourceManager().getAvailableResource());
          }
        } catch (Throwable e) {
          LOG.fatal(e.getMessage(), e);
          getTaskExecutor().releaseResource(taskStartEvent.getAllocation());
          getWorkerContext().getTaskManager().getDispatcher().getEventHandler()
              .handle(new ExecutionBlockErrorEvent(taskStartEvent.getExecutionBlockId(), e));
          break;
        }
        getTaskExecutor().handle(taskStartEvent);
        break;
      }
      case EB_STOP: {
        //receive event from QueryMaster
        ExecutionBlockStopEvent executionBlockStopEvent = TUtil.checkTypeAndGet(event, ExecutionBlockStopEvent.class);
        workerContext.getNodeResourceManager().getDispatcher().getEventHandler()
            .handle(new NodeStatusEvent(NodeStatusEvent.EventType.FLUSH_REPORTS));
        stopExecutionBlock(executionBlockContextMap.remove(executionBlockStopEvent.getExecutionBlockId()),
            executionBlockStopEvent.getCleanupList());
        break;
      }
      case QUERY_STOP: {
        QueryStopEvent queryStopEvent = TUtil.checkTypeAndGet(event, QueryStopEvent.class);

        //cleanup failure ExecutionBlock
        executionBlockContextMap.keySet().parallelStream()
          .filter(ebId -> ebId.getQueryId().equals(queryStopEvent.getQueryId())).forEach(ebId -> {
          try {
            executionBlockContextMap.remove(ebId).stop();
          } catch (Exception e) {
            LOG.fatal(e.getMessage(), e);
          }
        });
        workerContext.cleanup(queryStopEvent.getQueryId().toString());
        break;
      }
      case EB_FAIL: {
        ExecutionBlockErrorEvent errorEvent = TUtil.checkTypeAndGet(event, ExecutionBlockErrorEvent.class);
        LOG.error(errorEvent.getError().getMessage(), errorEvent.getError());
        ExecutionBlockContext context = executionBlockContextMap.remove(errorEvent.getExecutionBlockId());

        if (context != null) {
          context.getSharedResource().releaseBroadcastCache(context.getExecutionBlockId());
          getWorkerContext().getTaskHistoryWriter().flushTaskHistories();
          context.stop();
        }
        break;
      }
      default:
        break;
    }
  }

  protected ExecutionBlockContext getExecutionBlockContext(ExecutionBlockId executionBlockId) {
    return executionBlockContextMap.get(executionBlockId);
  }

  public Task getTaskByTaskAttemptId(TaskAttemptId taskAttemptId) {
    ExecutionBlockContext context = executionBlockContextMap.get(taskAttemptId.getTaskId().getExecutionBlockId());
    if (context != null) {
      return context.getTask(taskAttemptId);
    }
    return null;
  }

  public List<org.apache.tajo.util.history.TaskHistory> getTaskHistories(ExecutionBlockId executionblockId)
      throws IOException {

    return getWorkerContext().getHistoryReader().getTaskHistory(executionblockId.getQueryId().toString(),
        executionblockId.toString());
  }

  public TaskHistory getTaskHistory(TaskId taskId) throws IOException {
    TaskHistory history = null;
    ExecutionBlockContext context = executionBlockContextMap.get(taskId.getExecutionBlockId());
    if (context != null) {
      history = context.getTaskHistories().get(taskId);
    }
    //TODO get TaskHistory from HistoryReader
    return history;
  }
}
