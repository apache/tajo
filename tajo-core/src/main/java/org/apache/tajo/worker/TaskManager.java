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
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.worker.event.*;

import java.io.IOException;
import java.util.*;

/**
 * A TaskManager is responsible for managing executionBlock resource and tasks.
 * */
public class TaskManager extends AbstractService implements EventHandler<TaskManagerEvent> {
  private static final Log LOG = LogFactory.getLog(TaskManager.class);

  private final TajoWorker.WorkerContext workerContext;
  private final Map<ExecutionBlockId, ExecutionBlockContext> executionBlockContextMap;
  private final Dispatcher dispatcher;
  private final EventHandler rmEventHandler;

  private TajoConf tajoConf;

  public TaskManager(Dispatcher dispatcher, TajoWorker.WorkerContext workerContext, EventHandler rmEventHandler) {
    super(TaskManager.class.getName());

    this.dispatcher = dispatcher;
    this.workerContext = workerContext;
    this.executionBlockContextMap = Maps.newHashMap();
    this.rmEventHandler = rmEventHandler;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }

    this.tajoConf = (TajoConf)conf;
    dispatcher.register(TaskManagerEvent.EventType.class, this);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {

    for(ExecutionBlockContext context: executionBlockContextMap.values()) {
      context.stop();
    }
    executionBlockContextMap.clear();
    super.serviceStop();
  }

  protected Dispatcher getDispatcher() {
    return dispatcher;
  }

  protected TajoWorker.WorkerContext getWorkerContext() {
    return workerContext;
  }

  protected ExecutionBlockContext createExecutionBlock(TajoWorkerProtocol.RunExecutionBlockRequestProto request) {
    try {
      ExecutionBlockContext context = new ExecutionBlockContext(getWorkerContext(), null, request);

      context.init();
      return context;
    } catch (Throwable e) {
      LOG.fatal(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  protected void stopExecutionBlock(ExecutionBlockContext context,
                                    TajoWorkerProtocol.ExecutionBlockListProto cleanupList) {

    if(context != null){
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
    LOG.info("======================== Processing " + event.getExecutionBlockId() + " of type " + event.getType());

    if (event instanceof ExecutionBlockStartEvent) {

      //receive event from NodeResourceManager
      if(!executionBlockContextMap.containsKey(event.getExecutionBlockId())) {
        ExecutionBlockContext context = createExecutionBlock(((ExecutionBlockStartEvent) event).getRequestProto());
        executionBlockContextMap.put(context.getExecutionBlockId(), context);
      } else {
        LOG.warn("Already initialized ExecutionBlock: " + event.getExecutionBlockId());
      }
    } else if (event instanceof ExecutionBlockStopEvent) {
      //receive event from QueryMaster
      rmEventHandler.handle(new NodeStatusEvent(NodeStatusEvent.EventType.FLUSH_REPORTS));
      stopExecutionBlock(executionBlockContextMap.remove(event.getExecutionBlockId()),
          ((ExecutionBlockStopEvent) event).getCleanupList());
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

  public List<TaskHistory> getTaskHistories(ExecutionBlockId executionblockId) throws IOException {
    List<TaskHistory> histories = new ArrayList<TaskHistory>();
    ExecutionBlockContext context = executionBlockContextMap.get(executionblockId);
    if (context != null) {
      histories.addAll(context.getTaskHistories().values());
    }
    //TODO get List<TaskHistory> from HistoryReader
    return histories;
  }

  public TaskHistory getTaskHistory(TaskId taskId) {
    TaskHistory history = null;
    ExecutionBlockContext context = executionBlockContextMap.get(taskId.getExecutionBlockId());
    if (context != null) {
      history = context.getTaskHistories().get(taskId);
    }
    //TODO get TaskHistory from HistoryReader
    return history;
  }
}
