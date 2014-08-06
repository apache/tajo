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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.QueryUnitRequestImpl;
import org.apache.tajo.engine.utils.TupleCache;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.TajoIdUtils;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;

/**
 * The driver class for Tajo QueryUnit processing.
 */
public class TaskRunner extends AbstractService {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(TaskRunner.class);

  private TajoConf systemConf;

  private volatile boolean stopped = false;

  private ExecutionBlockId executionBlockId;
  private QueryId queryId;
  private NodeId nodeId;
  private ContainerId containerId;

  // Cluster Management
  //private TajoWorkerProtocol.TajoWorkerProtocolService.Interface master;

  // for temporal or intermediate files
  private FileSystem localFS;
  // for input files
  private FileSystem defaultFS;

  private TajoQueryEngine queryEngine;

  // for Fetcher
  private ExecutorService fetchLauncher;
  // It keeps all of the query unit attempts while a TaskRunner is running.
  private final Map<QueryUnitAttemptId, Task> tasks = new ConcurrentHashMap<QueryUnitAttemptId, Task>();

  private LocalDirAllocator lDirAllocator;

  // A thread to receive each assigned query unit and execute the query unit
  private Thread taskLauncher;

  // Contains the object references related for TaskRunner
  private TaskRunnerContext taskRunnerContext;
  // for the doAs block
  private UserGroupInformation taskOwner;

  // for the local temporal dir
  private String baseDir;
  private Path baseDirPath;

  private TaskRunnerManager taskRunnerManager;

  private long finishTime;

  private RpcConnectionPool connPool;

  private InetSocketAddress qmMasterAddr;

  private TaskRunnerHistory history;

  public TaskRunner(TaskRunnerManager taskRunnerManager, TajoConf conf, String[] args) {
    super(TaskRunner.class.getName());

    this.taskRunnerManager = taskRunnerManager;
    this.connPool = RpcConnectionPool.getPool(conf);
    this.fetchLauncher = Executors.newFixedThreadPool(
        conf.getIntVar(ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM));
    try {
      final ExecutionBlockId executionBlockId = TajoIdUtils.createExecutionBlockId(args[1]);

      LOG.info("Tajo Root Dir: " + conf.getVar(ConfVars.ROOT_DIR));
      LOG.info("Worker Local Dir: " + conf.getVar(ConfVars.WORKER_TEMPORAL_DIR));

      UserGroupInformation.setConfiguration(conf);

      // QueryBlockId from String
      // NodeId has a form of hostname:port.
      NodeId nodeId = ConverterUtils.toNodeId(args[2]);
      this.containerId = ConverterUtils.toContainerId(args[3]);


      // QueryMaster's address
      String host = args[4];
      int port = Integer.parseInt(args[5]);
      this.qmMasterAddr = NetUtils.createSocketAddrForHost(host, port);

      LOG.info("QueryMaster Address:" + qmMasterAddr);
      // TODO - 'load credential' should be implemented
      // Getting taskOwner
      UserGroupInformation taskOwner = UserGroupInformation.createRemoteUser(conf.getVar(ConfVars.USERNAME));
      //taskOwner.addToken(token);

      // initialize MasterWorkerProtocol as an actual task owner.
//      this.client =
//          taskOwner.doAs(new PrivilegedExceptionAction<AsyncRpcClient>() {
//            @Override
//            public AsyncRpcClient run() throws Exception {
//              return new AsyncRpcClient(TajoWorkerProtocol.class, masterAddr);
//            }
//          });
//      this.master = client.getStub();

      this.executionBlockId = executionBlockId;
      this.queryId = executionBlockId.getQueryId();
      this.nodeId = nodeId;
      this.taskOwner = taskOwner;

      this.taskRunnerContext = new TaskRunnerContext();
      this.history = new TaskRunnerHistory(containerId, executionBlockId);
      this.history.setState(getServiceState());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public String getId() {
    return getId(executionBlockId, containerId);
  }

  public static String getId(ExecutionBlockId executionBlockId, ContainerId containerId) {
    return executionBlockId + "," + containerId;
  }

  public static Path getBaseOutputDir(ExecutionBlockId executionBlockId){
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

  @Override
  public void init(Configuration conf) {
    this.systemConf = (TajoConf)conf;

    try {
      // initialize DFS and LocalFileSystems
      defaultFS = TajoConf.getTajoRootDir(systemConf).getFileSystem(conf);
      localFS = FileSystem.getLocal(conf);

      // the base dir for an output dir
      baseDir = getBaseOutputDir(executionBlockId).toString();

      // initialize LocalDirAllocator
      lDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);

      baseDirPath = localFS.makeQualified(lDirAllocator.getLocalPathForWrite(baseDir, conf));
      LOG.info("TaskRunner basedir is created (" + baseDir +")");

      // Setup QueryEngine according to the query plan
      // Here, we can setup row-based query engine or columnar query engine.
      this.queryEngine = new TajoQueryEngine(systemConf);
    } catch (Throwable t) {
      t.printStackTrace();
      LOG.error(t);
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
    this.stopped = true;

    // If TaskRunner is stopped, all running or pending tasks will be marked as failed.
    for (Task task : tasks.values()) {
      if (task.getStatus() == TaskAttemptState.TA_PENDING ||
          task.getStatus() == TaskAttemptState.TA_RUNNING) {
        task.setState(TaskAttemptState.TA_FAILED);
        task.abort();
      }
    }

    tasks.clear();
    fetchLauncher.shutdown();
    fetchLauncher = null;
    this.queryEngine = null;

    TupleCache.getInstance().removeBroadcastCache(executionBlockId);

    LOG.info("Stop TaskRunner: " + executionBlockId);
    synchronized (this) {
      notifyAll();
    }
    super.stop();
    this.history.setState(getServiceState());
  }

  public long getFinishTime() {
    return finishTime;
  }

  public class TaskRunnerContext {
    public AtomicInteger completedTasksNum = new AtomicInteger();
    public AtomicInteger succeededTasksNum = new AtomicInteger();
    public AtomicInteger killedTasksNum = new AtomicInteger();
    public AtomicInteger failedTasksNum = new AtomicInteger();

    public TajoConf getConf() {
      return systemConf;
    }

    public String getNodeId() {
      return nodeId.toString();
    }

    public FileSystem getLocalFS() {
      return localFS;
    }

    public FileSystem getDefaultFS() {
      return defaultFS;
    }

    public LocalDirAllocator getLocalDirAllocator() {
      return lDirAllocator;
    }

    public TajoQueryEngine getTQueryEngine() {
      return queryEngine;
    }

    public Map<QueryUnitAttemptId, Task> getTasks() {
      return tasks;
    }

    public Task getTask(QueryUnitAttemptId taskId) {
      return tasks.get(taskId);
    }

    public ExecutorService getFetchLauncher() {
      return fetchLauncher;
    }

    public Path getBaseDir() {
      return baseDirPath;
    }

    public ExecutionBlockId getExecutionBlockId() {
      return executionBlockId;
    }

    public void addTaskHistory(QueryUnitAttemptId quAttemptId, TaskHistory taskHistory) {
      history.addTaskHistory(quAttemptId, taskHistory);
    }

    public TaskRunnerHistory getExcutionBlockHistory(){
      return history;
    }
  }

  public TaskRunnerContext getContext() {
    return taskRunnerContext;
  }

  static void fatalError(QueryMasterProtocolService.Interface qmClientService,
                         QueryUnitAttemptId taskAttemptId, String message) {
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
          CallFuture<QueryUnitRequestProto> callFuture = null;
          QueryUnitRequestProto taskRequest = null;

          while(!stopped) {
            NettyClientBase qmClient = null;
            QueryMasterProtocolService.Interface qmClientService = null;
            try {
              qmClient = connPool.getConnection(qmMasterAddr, QueryMasterProtocol.class, true);
              qmClientService = qmClient.getStub();

              if (callFuture == null) {
                callFuture = new CallFuture<QueryUnitRequestProto>();
                LOG.info("Request GetTask: " + getId());
                GetTaskRequestProto request = GetTaskRequestProto.newBuilder()
                    .setExecutionBlockId(executionBlockId.getProto())
                    .setContainerId(((ContainerIdPBImpl) containerId).getProto())
                    .build();

                qmClientService.getTask(null, request, callFuture);
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
                // if there has been no assigning task for a given period,
                // TaskRunner will retry to request an assigning task.
                LOG.info("Retry assigning task:" + getId());
                continue;
              }

              if (taskRequest != null) {
                // QueryMaster can send the terminal signal to TaskRunner.
                // If TaskRunner receives the terminal signal, TaskRunner will be terminated
                // immediately.
                if (taskRequest.getShouldDie()) {
                  LOG.info("Received ShouldDie flag:" + getId());
                  stop();
                  if(taskRunnerManager != null) {
                    //notify to TaskRunnerManager
                    taskRunnerManager.stopTask(getId());
                    taskRunnerManager= null;
                  }
                } else {
                  taskRunnerManager.getWorkerContext().getWorkerSystemMetrics().counter("query", "task").inc();
                  LOG.info("Accumulated Received Task: " + (++receivedNum));

                  QueryUnitAttemptId taskAttemptId = new QueryUnitAttemptId(taskRequest.getId());
                  if (tasks.containsKey(taskAttemptId)) {
                    LOG.error("Duplicate Task Attempt: " + taskAttemptId);
                    fatalError(qmClientService, taskAttemptId, "Duplicate Task Attempt: " + taskAttemptId);
                    continue;
                  }

                  LOG.info("Initializing: " + taskAttemptId);
                  Task task;
                  try {
                    task = new Task(taskAttemptId, taskRunnerContext, qmClientService,
                        new QueryUnitRequestImpl(taskRequest));
                    tasks.put(taskAttemptId, task);

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
              }
            } catch (Throwable t) {
              t.printStackTrace();
            } finally {
              connPool.releaseConnection(qmClient);
            }
          }
        }
      });
      taskLauncher.start();
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
    } finally {
      for (Task t : tasks.values()) {
        if (t.getStatus() != TaskAttemptState.TA_SUCCEEDED) {
          t.abort();
        }
      }
    }
  }

  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopped() {
    return this.stopped;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return this.executionBlockId;
  }
}
