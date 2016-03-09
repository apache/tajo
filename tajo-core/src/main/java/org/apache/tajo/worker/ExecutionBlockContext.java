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
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpHeaders.Values;
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
import org.apache.tajo.exception.ErrorUtil;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.rpc.*;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.util.Pair;
import org.apache.tajo.worker.event.ExecutionBlockErrorEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ResourceProtos.*;
import static org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService.Interface;

public class ExecutionBlockContext {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(ExecutionBlockContext.class);

  protected AtomicInteger runningTasksNum = new AtomicInteger();
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

  private final ExecutionBlockSharedResource resource;

  private TajoQueryEngine queryEngine;
  private RpcClientManager connManager;
  private AsyncRpcClient queryMasterClient;
  private QueryMasterProtocol.QueryMasterProtocolService.Interface stub;
  private TajoConf systemConf;
  // for the doAs block
  private UserGroupInformation taskOwner;

  private Reporter reporter;

  private AtomicBoolean stop = new AtomicBoolean();

  private PlanProto.ShuffleType shuffleType;

  // It keeps all of the query unit attempts while a TaskRunner is running.
  private final ConcurrentMap<TaskAttemptId, Task> tasks = Maps.newConcurrentMap();

  private final Map<TaskId, TaskHistory> taskHistories = Maps.newConcurrentMap();

  public ExecutionBlockContext(TajoWorker.WorkerContext workerContext, ExecutionBlockContextResponse request,
                               AsyncRpcClient queryMasterClient)
      throws IOException {
    this.executionBlockId = new ExecutionBlockId(request.getExecutionBlockId());
    this.connManager = RpcClientManager.getInstance();
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
    this.queryMasterClient = queryMasterClient;
  }

  public void init() throws Throwable {

    LOG.info("Tajo Root Dir: " + systemConf.getVar(TajoConf.ConfVars.ROOT_DIR));
    LOG.info("Worker Local Dir: " + systemConf.getVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR));


    UserGroupInformation.setConfiguration(systemConf);
    // TODO - 'load credential' should be implemented
    // Getting taskOwner
    UserGroupInformation
        taskOwner = UserGroupInformation.createRemoteUser(systemConf.getVar(TajoConf.ConfVars.USERNAME));

    // initialize DFS and LocalFileSystems
    this.taskOwner = taskOwner;
    this.stub = queryMasterClient.getStub();
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

  private AsyncRpcClient getRpcClient() {
    return queryMasterClient;
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

    LOG.info("Worker's task counter - total:" + completedTasksNum.intValue() +
        ", succeeded: " + succeededTasksNum.intValue()
        + ", killed: " + killedTasksNum.intValue()
        + ", failed: " + failedTasksNum.intValue());

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

    // Clear index cache in pull server
    clearIndexCache();

    resource.release();
    RpcClientManager.cleanup(queryMasterClient);
  }

  /**
   * Send a request to {@link TajoPullServerService} to clear index cache
   */
  private void clearIndexCache() {
    // Avoid unnecessary cache clear request when the current eb is a leaf eb
    if (executionBlockId.getId() > 1) {
      Bootstrap bootstrap = new Bootstrap()
          .group(NettyUtils.getSharedEventLoopGroup(NettyUtils.GROUP.FETCHER, 1))
          .channel(NioSocketChannel.class)
          .option(ChannelOption.ALLOCATOR, NettyUtils.ALLOCATOR)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30 * 1000)
          .option(ChannelOption.TCP_NODELAY, true);
      ChannelInitializer<Channel> initializer = new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          pipeline.addLast("codec", new HttpClientCodec());
        }
      };
      bootstrap.handler(initializer);

      WorkerConnectionInfo connInfo = workerContext.getConnectionInfo();
      ChannelFuture future = bootstrap.connect(new InetSocketAddress(connInfo.getHost(), connInfo.getPullServerPort()))
          .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

      try {
        Channel channel = future.await().channel();
        if (!future.isSuccess()) {
          // Upon failure to connect to pull server, cache clear message is just ignored.
          LOG.warn(future.cause());
          return;
        }

        // Example of URI: /ebid=eb_1450063997899_0015_000002
        ExecutionBlockId clearEbId = new ExecutionBlockId(executionBlockId.getQueryId(), executionBlockId.getId() - 1);
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, "ebid=" + clearEbId.toString());
        request.headers().set(Names.HOST, connInfo.getHost());
        request.headers().set(Names.CONNECTION, Values.CLOSE);
        channel.writeAndFlush(request);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        if (future != null && future.channel().isOpen()) {
          // Close the channel to exit.
          future.channel().close();
        }
      }
    }
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
    return TajoPullServerService.getBaseOutputDir(
        executionBlockId.getQueryId().toString(), String.valueOf(executionBlockId.getId()));
  }

  public static Path getBaseInputDir(ExecutionBlockId executionBlockId) {
    return TajoPullServerService.getBaseInputDir(executionBlockId.getQueryId().toString(), executionBlockId.toString());
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

  public void addTaskHistory(TaskId taskId, TaskHistory taskHistory) {
    taskHistories.put(taskId, taskHistory);
  }

  public Map<TaskId, TaskHistory> getTaskHistories() {
    return taskHistories;
  }

  public void fatalError(TaskAttemptId taskAttemptId, Throwable error) {
    if (error == null) {
      error = new TajoInternalError("No error message");
    }
    TaskFatalErrorReport.Builder builder = TaskFatalErrorReport.newBuilder()
        .setId(taskAttemptId.getProto())
        .setError(ErrorUtil.convertException(error));

    try {
      //If QueryMaster does not responding, current execution block should be stop
      CallFuture<PrimitiveProtos.NullProto> callFuture = new CallFuture<>();
      getStub().fatalError(callFuture.getController(), builder.build(), callFuture);
      callFuture.get();
    } catch (Exception e) {
      getWorkerContext().getTaskManager().getDispatcher().getEventHandler()
          .handle(new ExecutionBlockErrorEvent(taskAttemptId.getTaskId().getExecutionBlockId(), e));
    }
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

        CallFuture<PrimitiveProtos.NullProto> callFuture = new CallFuture<>();
        stub.doneExecutionBlock(callFuture.getController(), reporterBuilder.build(), callFuture);
        callFuture.get();
        return;
      }

      IntermediateEntryProto.Builder intermediateBuilder = IntermediateEntryProto.newBuilder();
      IntermediateEntryProto.PageProto.Builder pageBuilder = IntermediateEntryProto.PageProto.newBuilder();
      FailureIntermediateProto.Builder failureBuilder = FailureIntermediateProto.newBuilder();

      for (HashShuffleAppenderManager.HashShuffleIntermediate eachShuffle: shuffles) {
        List<IntermediateEntryProto.PageProto> pages = Lists.newArrayList();
        List<FailureIntermediateProto> failureIntermediateItems = Lists.newArrayList();

        for (Pair<Long, Integer> eachPage: eachShuffle.getPages()) {
          pageBuilder.clear();
          pageBuilder.setPos(eachPage.getFirst());
          pageBuilder.setLength(eachPage.getSecond());
          pages.add(pageBuilder.build());
        }

        for(Pair<Long, Pair<Integer, Integer>> eachFailure: eachShuffle.getFailureTskTupleIndexes()) {
          failureBuilder.clear();
          failureBuilder.setPagePos(eachFailure.getFirst());
          failureBuilder.setStartRowNum(eachFailure.getSecond().getFirst());
          failureBuilder.setEndRowNum(eachFailure.getSecond().getSecond());
          failureIntermediateItems.add(failureBuilder.build());
        }
        intermediateBuilder.clear();

        intermediateBuilder.setEbId(ebId.getProto())
            .setHost(getWorkerContext().getConnectionInfo().getHost() + ":" +
                getWorkerContext().getConnectionInfo().getPullServerPort())
            .setTaskId(-1)
            .setAttemptId(-1)
            .setPartId(eachShuffle.getPartId())
            .setVolume(eachShuffle.getVolume())
            .addAllPages(pages)
            .addAllFailures(failureIntermediateItems);
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
      CallFuture<PrimitiveProtos.NullProto> callFuture = new CallFuture<>();
      stub.doneExecutionBlock(callFuture.getController(), reporterBuilder.build(), callFuture);
      callFuture.get();
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
                for (Task task : new ArrayList<>(tasks.values())){

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
