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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.worker.event.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class TaskManager extends AbstractService implements EventHandler<TaskManagerEvent> {
  private static final Log LOG = LogFactory.getLog(TaskManager.class);

  private final TajoWorker.WorkerContext workerContext;
  private final Map<ExecutionBlockId, ExecutionBlockContext> executionBlockContextMap;
  private final Dispatcher dispatcher;
  private final EventHandler rmEventHandler;

  private TajoConf tajoConf;
  private volatile boolean isStopped = false;

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
    isStopped = true;

    for(ExecutionBlockContext context: executionBlockContextMap.values()) {
      context.stop();
    }
    executionBlockContextMap.clear();
    super.serviceStop();
  }

  protected Dispatcher getDispatcher() {
    return dispatcher;
  }

  protected void startExecutionBlock(ExecutionBlockStartEvent event){
    ExecutionBlockContext context = executionBlockContextMap.get(event.getExecutionBlockId());

    if(context == null){
      TajoWorkerProtocol.RunExecutionBlockRequestProto request = event.getRequestProto();
      try {
        context = new ExecutionBlockContext(workerContext, null, request);

        context.init();
      } catch (Throwable e) {
        LOG.fatal(e.getMessage(), e);
        throw new RuntimeException(e);
      }
      executionBlockContextMap.put(event.getExecutionBlockId(), context);
    }
  }

  protected void stopExecutionBlock(ExecutionBlockStopEvent event) {
    ExecutionBlockContext executionBlockContext =  executionBlockContextMap.remove(event.getExecutionBlockId());
    if(executionBlockContext != null){
      try {
        executionBlockContext.getSharedResource().releaseBroadcastCache(event.getExecutionBlockId());
        executionBlockContext.sendShuffleReport();
        workerContext.getTaskHistoryWriter().flushTaskHistories();
      } catch (Exception e) {
        LOG.fatal(e.getMessage(), e);
        throw new RuntimeException(e);
      } finally {
        executionBlockContext.stop();

          /* cleanup intermediate files */
        List<TajoIdProtos.ExecutionBlockIdProto> cleanupList = event.getCleanupList().getExecutionBlockIdList();
        for (TajoIdProtos.ExecutionBlockIdProto ebId : cleanupList) {
          String inputDir = ExecutionBlockContext.getBaseInputDir(new ExecutionBlockId(ebId)).toString();
          workerContext.cleanup(inputDir);
          String outputDir = ExecutionBlockContext.getBaseOutputDir(new ExecutionBlockId(ebId)).toString();
          workerContext.cleanup(outputDir);
        }
      }
    }
    LOG.info("Stopped execution block:" + event.getExecutionBlockId());
  }

  @Override
  public void handle(TaskManagerEvent event) {
    LOG.info("======================== Processing " + event.getExecutionBlockId() + " of type " + event.getType());

    if (event instanceof ExecutionBlockStartEvent) {

      //trigger event from NodeResourceManager
      startExecutionBlock((ExecutionBlockStartEvent) event);
    } else if (event instanceof ExecutionBlockStopEvent) {
      //trigger event from QueryMaster
      rmEventHandler.handle(new NodeStatusEvent(NodeStatusEvent.EventType.FLUSH_REPORTS));
      stopExecutionBlock((ExecutionBlockStopEvent) event);
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
