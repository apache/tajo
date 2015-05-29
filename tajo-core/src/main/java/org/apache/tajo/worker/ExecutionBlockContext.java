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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.rpc.*;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.NetUtils;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;
import static org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService.Interface;

public class ExecutionBlockContext {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(ExecutionBlockContext.class);

  private TaskRunnerManager manager;
  protected AtomicInteger completedTasksNum = new AtomicInteger();
  protected AtomicInteger succeededTasksNum = new AtomicInteger();
  protected AtomicInteger killedTasksNum = new AtomicInteger();
  protected AtomicInteger failedTasksNum = new AtomicInteger();

  private FileSystem localFS;
  // for input files
  private FileSystem defaultFS;
  private ExecutionBlockId executionBlockId;
  private QueryContext queryContext;
  private TajoWorker.WorkerContext workerContext;
  private String plan;

  private ExecutionBlockSharedResource resource;

  private TajoQueryEngine queryEngine;
  private RpcClientManager connManager;
  private InetSocketAddress qmMasterAddr;
  private NettyClientBase client;
  private QueryMasterProtocol.QueryMasterProtocolService.Interface stub;
  private WorkerConnectionInfo queryMaster;
  private TajoConf systemConf;
  // for the doAs block
  private UserGroupInformation taskOwner;

  private Reporter reporter;

  private AtomicBoolean stop = new AtomicBoolean();

  private PlanProto.ShuffleType shuffleType;

  // It keeps all of the query unit attempts while a TaskRunner is running.
  private final ConcurrentMap<TaskAttemptId, Task> tasks = Maps.newConcurrentMap();

  @Deprecated
  private final ConcurrentMap<String, TaskRunnerHistory> histories = Maps.newConcurrentMap();

  private final Map<TaskId, TaskHistory> taskHistories = Maps.newTreeMap();

  public ExecutionBlockContext(TajoWorker.WorkerContext workerContext,
                               TaskRunnerManager manager, RunExecutionBlockRequestProto request) throws IOException {
    this.manager = manager;
    this.executionBlockId = new ExecutionBlockId(request.getExecutionBlockId());
    this.connManager = RpcClientManager.getInstance();
    this.queryMaster = new WorkerConnectionInfo(request.getQueryMaster());
    this.systemConf = workerContext.getConf();
    this.reporter = new Reporter();
    this.defaultFS = TajoConf.getTajoRootDir(systemConf).getFileSystem(systemConf);
    this.localFS = FileSystem.getLocal(systemConf);

    // Setup QueryEngine according to the query plan
    // Here, we can setup row-based query engine or columnar query engine.
    this.queryEngine = new TajoQueryEngine(systemConf);
    this.queryContext = new QueryContext(workerContext.getConf(), request.getQueryContext());
    this.plan = request.getPlanJson();
    this.resource = new ExecutionBlockSharedResource();
    this.workerContext = workerContext;
    this.shuffleType = request.getShuffleType();
  }

  public void init() throws Throwable {

    LOG.info("Tajo Root Dir: " + systemConf.getVar(TajoConf.ConfVars.ROOT_DIR));
    LOG.info("Worker Local Dir: " + systemConf.getVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR));

    this.qmMasterAddr = NetUtils.createSocketAddr(queryMaster.getHost(), queryMaster.getQueryMasterPort());
    LOG.info("QueryMaster Address:" + qmMasterAddr);

    UserGroupInformation.setConfiguration(systemConf);
    // TODO - 'load credential' should be implemented
    // Getting taskOwner
    UserGroupInformation
        taskOwner = UserGroupInformation.createRemoteUser(systemConf.getVar(TajoConf.ConfVars.USERNAME));

    // initialize DFS and LocalFileSystems
    this.taskOwner = taskOwner;
    this.stub = getRpcClient().getStub();
    this.reporter.startReporter();
    // resource intiailization
    try{
      this.resource.initialize(queryContext, plan);
    } catch (Throwable e) {
      try {
        getStub().killQuery(null, executionBlockId.getQueryId().getProto(), NullCallback.get());
      } catch (Throwable t) {
        LOG.error(t);
      }
      throw e;
    }
  }

  public ExecutionBlockSharedResource getSharedResource() {
    return resource;
  }

  private NettyClientBase getRpcClient()
      throws NoSuchMethodException, ConnectException, ClassNotFoundException {
    if (client != null) return client;

    client = connManager.newClient(qmMasterAddr, QueryMasterProtocol.class, true);
    return client;
  }

  public Interface getStub() {
    return stub;
  }

  public boolean isStopped() {
    return stop.get();
  }

  public void stop(){
    if(stop.getAndSet(true)){
      return;
    }

    try {
      reporter.stop();
    } catch (InterruptedException e) {
      LOG.error(e);
    }

    // If ExecutionBlock is stopped, all running or pending tasks will be marked as failed.
    for (Task task : tasks.values()) {
      if (task.getTaskContext().getState() == TajoProtos.TaskAttemptState.TA_PENDING ||
          task.getTaskContext().getState() == TajoProtos.TaskAttemptState.TA_RUNNING) {

        try{
          task.abort();
        } catch (Throwable e){
          LOG.error(e, e);
        }
      }
    }
    tasks.clear();
    taskHistories.clear();
    resource.release();
    RpcClientManager.cleanup(client);
  }

  public TajoConf getConf() {
    return systemConf;
  }

  public FileSystem getLocalFS() {
    return localFS;
  }

  public LocalDirAllocator getLocalDirAllocator() {
    return workerContext.getLocalDirAllocator();
  }

  public TajoQueryEngine getTQueryEngine() {
    return queryEngine;
  }

  // for the local temporal dir
  public Path createBaseDir() throws IOException {
    // the base dir for an output dir
    String baseDir = getBaseOutputDir(executionBlockId).toString();
    Path baseDirPath = localFS.makeQualified(getLocalDirAllocator().getLocalPathForWrite(baseDir, systemConf));
    return baseDirPath;
  }

  public static Path getBaseOutputDir(ExecutionBlockId executionBlockId) {
    Path workDir =
        StorageUtil.concatPath(
            executionBlockId.getQueryId().toString(),
            "output",
            String.valueOf(executionBlockId.getId()));
    return workDir;
  }

  public static Path getBaseInputDir(ExecutionBlockId executionBlockId) {
    Path workDir =
        StorageUtil.concatPath(
            executionBlockId.getQueryId().toString(),
            "in",
            executionBlockId.toString());
    return workDir;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public Map<TaskAttemptId, Task> getTasks() {
    return tasks;
  }

  public Task getTask(TaskAttemptId taskAttemptId){
    return tasks.get(taskAttemptId);
  }

  @Deprecated
  public void stopTaskRunner(String id){
    manager.stopTaskRunner(id);
  }

  @Deprecated
  public TaskRunner getTaskRunner(String taskRunnerId){
    return manager.getTaskRunner(taskRunnerId);
  }

  @Deprecated
  public void addTaskHistory(String taskRunnerId, TaskAttemptId quAttemptId, TaskHistory taskHistory) {
    histories.get(taskRunnerId).addTaskHistory(quAttemptId, taskHistory);
  }

  public void addTaskHistory(TaskId taskId, TaskHistory taskHistory) {
    taskHistories.put(taskId, taskHistory);
  }

  public Map<TaskId, TaskHistory> getTaskHistories() {
    return taskHistories;
  }

  public void fatalError(TaskAttemptId taskAttemptId, String message) {
    if (message == null) {
      message = "No error message";
    }
    TaskFatalErrorReport.Builder builder = TaskFatalErrorReport.newBuilder()
        .setId(taskAttemptId.getProto())
        .setErrorMessage(message);

    getStub().fatalError(null, builder.build(), NullCallback.get());
  }

  public TaskRunnerHistory createTaskRunnerHistory(TaskRunner runner){
    histories.putIfAbsent(runner.getId(), new TaskRunnerHistory(runner.getContainerId(), executionBlockId));
    return histories.get(runner.getId());
  }

  public TajoWorker.WorkerContext getWorkerContext(){
    return workerContext;
  }

  /**
   * HASH_SHUFFLE, SCATTERED_HASH_SHUFFLE should send report when this executionBlock stopping.
   */
  protected void sendShuffleReport() throws Exception {

    switch (shuffleType) {
      case HASH_SHUFFLE:
      case SCATTERED_HASH_SHUFFLE:
        sendHashShuffleReport(executionBlockId);
        break;
      case NONE_SHUFFLE:
      case RANGE_SHUFFLE:
      default:
        break;
    }
  }

  private void sendHashShuffleReport(ExecutionBlockId ebId) throws Exception {
    /* This case is that worker did not ran tasks */
    if(completedTasksNum.get() == 0) return;

    Interface stub = getStub();

    ExecutionBlockReport.Builder reporterBuilder = ExecutionBlockReport.newBuilder();
    reporterBuilder.setEbId(ebId.getProto());
    reporterBuilder.setReportSuccess(true);
    reporterBuilder.setSucceededTasks(succeededTasksNum.get());
    try {
      List<IntermediateEntryProto> intermediateEntries = Lists.newArrayList();
      List<HashShuffleAppenderManager.HashShuffleIntermediate> shuffles =
          getWorkerContext().getHashShuffleAppenderManager().close(ebId);
      if (shuffles == null) {
        reporterBuilder.addAllIntermediateEntries(intermediateEntries);

        CallFuture<PrimitiveProtos.NullProto> callFuture = new CallFuture<PrimitiveProtos.NullProto>();
        stub.doneExecutionBlock(callFuture.getController(), reporterBuilder.build(), callFuture);
        callFuture.get(RpcConstants.DEFAULT_FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return;
      }

      IntermediateEntryProto.Builder intermediateBuilder = IntermediateEntryProto.newBuilder();

      WorkerConnectionInfo connectionInfo = getWorkerContext().getConnectionInfo();
      String address = connectionInfo.getHost() + ":" + connectionInfo.getPullServerPort();
      for (HashShuffleAppenderManager.HashShuffleIntermediate eachShuffle: shuffles) {
        intermediateBuilder.setEbId(ebId.getProto())
            .setAddress(address)
            .setTaskId(-1)
            .setAttemptId(-1)
            .setPartId(eachShuffle.getPartId())
            .setVolume(eachShuffle.getVolume())
            .addAllPages(eachShuffle.getPages())
            .addAllFailures(eachShuffle.getFailureTskTupleIndexes());
        intermediateEntries.add(intermediateBuilder.build());
      }

      // send intermediateEntries to QueryMaster
      reporterBuilder.addAllIntermediateEntries(intermediateEntries);

    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      reporterBuilder.setReportSuccess(false);
      if (e.getMessage() == null) {
        reporterBuilder.setReportErrorMessage(e.getClass().getSimpleName());
      } else {
        reporterBuilder.setReportErrorMessage(e.getMessage());
      }
    }
    try {
      CallFuture<PrimitiveProtos.NullProto> callFuture = new CallFuture<PrimitiveProtos.NullProto>();
      stub.doneExecutionBlock(callFuture.getController(), reporterBuilder.build(), callFuture);
      callFuture.get(RpcConstants.DEFAULT_FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (Throwable e) {
      // can't send report to query master
      LOG.fatal(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  protected class Reporter {
    private Thread reporterThread;
    private static final int PROGRESS_INTERVAL = 1000;
    private static final int MAX_RETRIES = 10;

    public Reporter() {
      this.reporterThread = new Thread(createReporterThread());
      this.reporterThread.setName("Task reporter");
    }

    public void startReporter(){
      this.reporterThread.start();
    }

    Runnable createReporterThread() {

      return new Runnable() {
        int remainingRetries = MAX_RETRIES;
        @Override
        public void run() {
          while (!isStopped() && !Thread.interrupted()) {

            try {
              Interface masterStub = getStub();

              if(tasks.size() == 0){
                masterStub.ping(null, getExecutionBlockId().getProto(), NullCallback.get());
              } else {
                for (Task task : new ArrayList<Task>(tasks.values())){

                  if (task.getTaskContext().getState() ==
                      TajoProtos.TaskAttemptState.TA_RUNNING && task.isProgressChanged()) {
                    masterStub.statusUpdate(null, task.getReport(), NullCallback.get());
                  }
                  task.updateProgress();
                }
              }
            } catch (Throwable t) {
              LOG.error(t.getMessage(), t);
              remainingRetries -=1;
              if (remainingRetries == 0) {
                ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
                LOG.warn("Last retry, exiting ");
                throw new RuntimeException(t);
              }
            } finally {
              if (remainingRetries > 0 && !isStopped()) {
                synchronized (reporterThread) {
                  try {
                    reporterThread.wait(PROGRESS_INTERVAL);
                  } catch (InterruptedException e) {
                  }
                }
              }
            }
          }
        }
      };
    }

    public void stop() throws InterruptedException {
      if (reporterThread != null) {
        // Intent of the lock is to not send an interupt in the middle of an
        // umbilical.ping or umbilical.statusUpdate
        synchronized (reporterThread) {
          //Interrupt if sleeping. Otherwise wait for the RPC call to return.
          reporterThread.notifyAll();
        }
      }
    }
  }
}
