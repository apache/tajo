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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.TaskRequestImpl;
import org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService;
import org.apache.tajo.master.container.TajoContainerId;
import org.apache.tajo.master.container.TajoContainerIdPBImpl;
import org.apache.tajo.master.container.TajoConverterUtils;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;

import io.netty.channel.ConnectTimeoutException;

import java.util.concurrent.*;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;

/**
 * The driver class for Tajo Task processing.
 */
public class TaskRunner extends AbstractService {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(TaskRunner.class);

  private TajoConf systemConf;

  private volatile boolean stopped = false;
  private Path baseDirPath;

  private TajoContainerId containerId;

  // for Fetcher
  private ExecutorService fetchLauncher;

  // A thread to receive each assigned query unit and execute the query unit
  private Thread taskLauncher;

  // Contains the object references related for TaskRunner
  private ExecutionBlockContext executionBlockContext;

  private long finishTime;

  private TaskRunnerHistory history;

  public TaskRunner(ExecutionBlockContext executionBlockContext, String containerId) {
    super(TaskRunner.class.getName());

    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    ThreadFactory fetcherFactory = builder.setNameFormat("Fetcher executor #%d").build();
    this.systemConf = executionBlockContext.getConf();
    this.fetchLauncher = Executors.newFixedThreadPool(
        systemConf.getIntVar(ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM), fetcherFactory);
    try {
      this.containerId = TajoConverterUtils.toTajoContainerId(containerId);
      this.executionBlockContext = executionBlockContext;
      this.history = executionBlockContext.createTaskRunnerHistory(this);
      this.history.setState(getServiceState());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  // TODO this is expensive. we should change to unique id
  public String getId() {
    return getId(getContext().getExecutionBlockId(), containerId);
  }

  public TajoContainerId getContainerId(){
    return containerId;
  }

  public static String getId(ExecutionBlockId executionBlockId, TajoContainerId containerId) {
    return executionBlockId + "," + containerId;
  }

  public TaskRunnerHistory getHistory(){
    return history;
  }

  public Path getTaskBaseDir(){
    return baseDirPath;
  }

  public ExecutorService getFetchLauncher() {
    return fetchLauncher;
  }

  @Override
  public void init(Configuration conf) {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("conf should be a TajoConf Type.");
    }
    this.systemConf = (TajoConf)conf;

    try {
      // the base dir for an output dir
      baseDirPath = getContext().createBaseDir();
      LOG.info("TaskRunner basedir is created (" + baseDirPath +")");
    } catch (Throwable t) {
      t.printStackTrace();
      LOG.error(t, t);
    }
    super.init(conf);
    this.history.setState(getServiceState());
  }

  @Override
  public void start() {
    super.start();
    history.setStartTime(getStartTime());
    this.history.setState(getServiceState());
    run();
  }

  @Override
  public void stop() {
    if(isStopped()) {
      return;
    }
    this.finishTime = System.currentTimeMillis();
    this.history.setFinishTime(finishTime);
    // If this flag become true, taskLauncher will be terminated.

    LOG.info("Stop TaskRunner: " + getId());
    synchronized (this) {
      this.stopped = true;

      fetchLauncher.shutdown();
      fetchLauncher = null;

      notifyAll();
    }

    super.stop();
    this.history.setState(getServiceState());
  }

  public long getFinishTime() {
    return finishTime;
  }

  public ExecutionBlockContext getContext() {
    return executionBlockContext;
  }

  static void fatalError(QueryMasterProtocolService.Interface qmClientService,
                         TaskAttemptId taskAttemptId, String message) {
    if (message == null) {
       message = "No error message";
    }
    TaskFatalErrorReport.Builder builder = TaskFatalErrorReport.newBuilder()
        .setId(taskAttemptId.getProto())
        .setErrorMessage(message);

    qmClientService.fatalError(null, builder.build(), NullCallback.get());
  }

  public void run() {
    LOG.info("TaskRunner startup");
    try {

      taskLauncher = new Thread(new Runnable() {

        @Override
        public void run() {
          int receivedNum = 0;
          CallFuture<TaskRequestProto> callFuture = null;
          TaskRequestProto taskRequest = null;

          while(!stopped) {
            NettyClientBase client;
            try {
              client = executionBlockContext.getQueryMasterConnection();
            } catch (ConnectTimeoutException ce) {
              // NettyClientBase throws ConnectTimeoutException if connection was failed
              stop();
              getContext().stopTaskRunner(getId());
              LOG.error("Connecting to QueryMaster was failed.", ce);
              break;
            } catch (Throwable t) {
              LOG.fatal("Unable to handle exception: " + t.getMessage(), t);
              stop();
              getContext().stopTaskRunner(getId());
              break;
            }

            QueryMasterProtocolService.Interface qmClientService = client.getStub();

            try {
              if (callFuture == null) {
                callFuture = new CallFuture<TaskRequestProto>();
                LOG.info("Request GetTask: " + getId());
                GetTaskRequestProto request = GetTaskRequestProto.newBuilder()
                    .setExecutionBlockId(getExecutionBlockId().getProto())
                    .setContainerId(((TajoContainerIdPBImpl) containerId).getProto())
                    .setWorkerId(getContext().getWorkerContext().getConnectionInfo().getId())
                    .build();

                qmClientService.getTask(callFuture.getController(), request, callFuture);
              }
              try {
                // wait for an assigning task for 3 seconds
                taskRequest = callFuture.get(3, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                if(stopped) {
                  break;
                }
              } catch (TimeoutException te) {
                if(stopped) {
                  break;
                }

                if(callFuture.getController().failed()){
                  LOG.error(callFuture.getController().errorText());
                  break;
                }
                // if there has been no assigning task for a given period,
                // TaskRunner will retry to request an assigning task.
                if (LOG.isDebugEnabled()) {
                  LOG.info("Retry assigning task:" + getId());
                }
                continue;
              }

              if (taskRequest != null) {
                // QueryMaster can send the terminal signal to TaskRunner.
                // If TaskRunner receives the terminal signal, TaskRunner will be terminated
                // immediately.
                if (taskRequest.getShouldDie()) {
                  LOG.info("Received ShouldDie flag:" + getId());
                  stop();
                  //notify to TaskRunnerManager
                  getContext().stopTaskRunner(getId());
                } else {
                  getContext().getWorkerContext().getWorkerSystemMetrics().counter("query", "task").inc();
                  LOG.info("Accumulated Received Task: " + (++receivedNum));

                  TaskAttemptId taskAttemptId = new TaskAttemptId(taskRequest.getId());
                  if (getContext().getTasks().containsKey(taskAttemptId)) {
                    LOG.error("Duplicate Task Attempt: " + taskAttemptId);
                    fatalError(qmClientService, taskAttemptId, "Duplicate Task Attempt: " + taskAttemptId);
                    continue;
                  }

                  LOG.info("Initializing: " + taskAttemptId);
                  Task task;
                  try {
                    task = new Task(getId(), getTaskBaseDir(), taskAttemptId, executionBlockContext,
                        new TaskRequestImpl(taskRequest));
                    getContext().getTasks().put(taskAttemptId, task);

                    task.init();
                    if (task.hasFetchPhase()) {
                      task.fetch(); // The fetch is performed in an asynchronous way.
                    }
                    // task.run() is a blocking call.
                    task.run();
                  } catch (Throwable t) {
                    LOG.error(t.getMessage(), t);
                    fatalError(qmClientService, taskAttemptId, t.getMessage());
                  } finally {
                    callFuture = null;
                    taskRequest = null;
                  }
                }
              } else {
                stop();
                //notify to TaskRunnerManager
                getContext().stopTaskRunner(getId());
              }
            } catch (Throwable t) {
              LOG.fatal(t.getMessage(), t);
            }
          }
        }
      });
      taskLauncher.start();
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
    }
  }

  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopped() {
    return this.stopped;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return getContext().getExecutionBlockId();
  }
}
