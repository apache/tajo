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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tajo.QueryConf;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.SubQueryId;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.MasterWorkerProtos.QueryUnitRequestProto;
import org.apache.tajo.engine.query.QueryUnitRequestImpl;
import org.apache.tajo.ipc.MasterWorkerProtocol;
import org.apache.tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService;
import org.apache.tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService.Interface;
import org.apache.tajo.rpc.CallFuture2;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.ProtoAsyncRpcClient;
import org.apache.tajo.util.TajoIdUtils;

import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.tajo.engine.MasterWorkerProtos.TaskFatalErrorReport;

/**
 * The driver class for Tajo QueryUnit processing.
 */
public class TaskRunner extends AbstractService {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(TaskRunner.class);

  private QueryConf conf;

  private volatile boolean stopped = false;

  private final SubQueryId subQueryId;
  private ApplicationId appId;
  private final NodeId nodeId;
  private final ContainerId containerId;

  // Cluster Management
  private MasterWorkerProtocolService.Interface master;

  // for temporal or intermediate files
  private FileSystem localFS;
  // for input files
  private FileSystem defaultFS;

  private TajoQueryEngine queryEngine;

  // TODO - this should be configurable
  private final int coreNum = 4;

  // for Fetcher
  private final ExecutorService fetchLauncher =
      Executors.newFixedThreadPool(coreNum * 4);
  // It keeps all of the query unit attempts while a TaskRunner is running.
  private final Map<QueryUnitAttemptId, Task> tasks =
      new ConcurrentHashMap<QueryUnitAttemptId, Task>();
  private LocalDirAllocator lDirAllocator;

  // A thread to receive each assigned query unit and execute the query unit
  private Thread taskLauncher;

  // Contains the object references related for TaskRunner
  private WorkerContext workerContext;
  // for the doAs block
  private UserGroupInformation taskOwner;

  // for the local temporal dir
  private String baseDir;
  private Path baseDirPath;

  public TaskRunner(
      final SubQueryId subQueryId,
      final NodeId nodeId,
      UserGroupInformation taskOwner,
      Interface master, ContainerId containerId) {
    super(TaskRunner.class.getName());
    this.subQueryId = subQueryId;
    this.appId = subQueryId.getQueryId().getApplicationId();
    this.nodeId = nodeId;
    this.taskOwner = taskOwner;
    this.master = master;
    this.containerId = containerId;
  }

  @Override
  public void init(Configuration _conf) {
    this.conf = (QueryConf) _conf;

    try {
      this.workerContext = new WorkerContext();

      // initialize DFS and LocalFileSystems
      defaultFS = FileSystem.get(URI.create(conf.getVar(ConfVars.ROOT_DIR)),conf);
      localFS = FileSystem.getLocal(conf);

      // the base dir for an output dir
      baseDir = ConverterUtils.toString(appId)
          + "/output" + "/" + subQueryId.getId();

      // initialize LocalDirAllocator
      lDirAllocator = new LocalDirAllocator(ConfVars.TASK_LOCAL_DIR.varname);

      baseDirPath = localFS.makeQualified(lDirAllocator.getLocalPathForWrite(baseDir, conf));
      LOG.info("TaskRunner basedir is created (" + baseDir +")");

      // Setup QueryEngine according to the query plan
      // Here, we can setup row-based query engine or columnar query engine.
      this.queryEngine = new TajoQueryEngine(conf);

      Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
    } catch (Throwable t) {
      LOG.error(t);
    }

    super.init(conf);
  }

  @Override
  public void start() {
    run();
  }

  @Override
  public void stop() {
    if (!isStopped()) {
      // If TaskRunner is stopped, all running or pending tasks will be marked as failed.
      for (Task task : tasks.values()) {
        if (task.getStatus() == TaskAttemptState.TA_PENDING ||
            task.getStatus() == TaskAttemptState.TA_RUNNING) {
          task.setState(TaskAttemptState.TA_FAILED);
        }
      }

      // If this flag become true, taskLauncher will be terminated.
      this.stopped = true;

      LOG.info("STOPPED: " + nodeId);
      synchronized (this) {
        notifyAll();
      }
    }
  }

  class WorkerContext {
    public QueryConf getConf() {
      return conf;
    }

    public String getNodeId() {
      return nodeId.toString();
    }

    public MasterWorkerProtocolService.Interface getMaster() {
      return master;
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
  }

  static void fatalError(MasterWorkerProtocolService.Interface proxy,
                         QueryUnitAttemptId taskAttemptId, String message) {
    TaskFatalErrorReport.Builder builder = TaskFatalErrorReport.newBuilder()
        .setId(taskAttemptId.getProto())
        .setErrorMessage(message);
    proxy.fatalError(null, builder.build(), NullCallback.get());
  }

  public void run() {
    LOG.info("TaskRunner startup");

    try {

      taskLauncher = new Thread(new Runnable() {
        @Override
        public void run() {
          int receivedNum = 0;
          CallFuture2<QueryUnitRequestProto> callFuture = null;
          QueryUnitRequestProto taskRequest = null;

          while(!stopped) {
            try {
              if (callFuture == null) {
                callFuture = new CallFuture2<QueryUnitRequestProto>();
                master.getTask(null, ((ContainerIdPBImpl) containerId).getProto(),
                    callFuture);
              }
              try {
                // wait for an assigning task for 3 seconds
                taskRequest = callFuture.get(3, TimeUnit.SECONDS);
              } catch (TimeoutException te) {
                // if there has been no assigning task for a given period,
                // TaskRunner will retry to request an assigning task.
                LOG.error(te);

                continue;
              }

              if (taskRequest != null) {
                // QueryMaster can send the terminal signal to TaskRunner.
                // If TaskRunner receives the terminal signal, TaskRunner will be terminated
                // immediately.
                if (taskRequest.getShouldDie()) {
                  LOG.info("received ShouldDie flag");
                  stop();

                } else {

                  LOG.info("Accumulated Received Task: " + (++receivedNum));

                  QueryUnitAttemptId taskAttemptId = new QueryUnitAttemptId(taskRequest.getId());
                  if (tasks.containsKey(taskAttemptId)) {
                    fatalError(master, taskAttemptId, "Duplicate Task Attempt: " + taskAttemptId);
                    continue;
                  }

                  LOG.info("Initializing: " + taskAttemptId);
                  Task task = new Task(taskAttemptId, workerContext, master,
                      new QueryUnitRequestImpl(taskRequest));
                  tasks.put(taskAttemptId, task);

                  task.init();
                  if (task.hasFetchPhase()) {
                    task.fetch(); // The fetch is performed in an asynchronous way.
                  }
                  // task.run() is a blocking call.
                  task.run();

                  callFuture = null;
                  taskRequest = null;
                }
              }
            } catch (Throwable t) {
              LOG.error(t);
            }
          }
        }
      });
      taskLauncher.start();
      taskLauncher.join();

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

  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      LOG.info("received SIGINT Signal");
      stop();
    }
  }

  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * TaskRunner takes 5 arguments as follows:
   * <ol>
   * <li>1st: TaskRunnerListener hostname</li>
   * <li>2nd: TaskRunnerListener port</li>
   * <li>3nd: SubQueryId</li>
   * <li>4th: NodeId</li>
   * <li>5th: ContainerId</li>
   * </ol>
   */
  public static void main(String[] args) throws Exception {
    // Restore QueryConf
    final QueryConf conf = new QueryConf();
    conf.addResource(new Path(QueryConf.FILENAME));

    LOG.info("MiniTajoYarn NM Local Dir: " + conf.get(ConfVars.TASK_LOCAL_DIR.varname));
    LOG.info("OUTPUT DIR: " + conf.getOutputPath());
    LOG.info("Tajo Root Dir: " + conf.getVar(ConfVars.ROOT_DIR));

    UserGroupInformation.setConfiguration(conf);

    // TaskRunnerListener's address
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress masterAddr =
        NetUtils.createSocketAddrForHost(host, port);

    // SubQueryId from String
    final SubQueryId subQueryId = TajoIdUtils.newSubQueryId(args[2]);
    // NodeId has a form of hostname:port.
    NodeId nodeId = ConverterUtils.toNodeId(args[3]);
    ContainerId containerId = ConverterUtils.toContainerId(args[4]);

    // TODO - 'load credential' should be implemented
    // Getting taskOwner
    UserGroupInformation taskOwner =
        UserGroupInformation.createRemoteUser(conf.getVar(ConfVars.QUERY_USERNAME));
    //taskOwner.addToken(token);

    // TaskRunnerListener RPC
    ProtoAsyncRpcClient client;
    MasterWorkerProtocolService.Interface master;

    // initialize MasterWorkerProtocol as an actual task owner.
    client =
        taskOwner.doAs(new PrivilegedExceptionAction<ProtoAsyncRpcClient>() {
          @Override
          public ProtoAsyncRpcClient run() throws Exception {
            return new ProtoAsyncRpcClient(MasterWorkerProtocol.class, masterAddr);
          }
        });
    master = client.getStub();


    TaskRunner taskRunner = new TaskRunner(subQueryId, nodeId, taskOwner, master, containerId);
    taskRunner.init(conf);
    taskRunner.start();
    client.close();
    LOG.info("TaskRunner (" + nodeId + ") main thread exiting");
    System.exit(0);
  }
}
