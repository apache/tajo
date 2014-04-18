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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.engine.planner.logical.SortNode;
import org.apache.tajo.engine.planner.physical.PhysicalExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.engine.query.QueryUnitRequest;
import org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService;
import org.apache.tajo.ipc.TajoWorkerProtocol.*;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcChannelFactory;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.ApplicationIdUtils;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class Task {
  private static final Log LOG = LogFactory.getLog(Task.class);
  private static final float FETCHER_PROGRESS = 0.5f;

  private final TajoConf systemConf;
  private final QueryContext queryContext;
  private final FileSystem localFS;
  private TaskRunner.TaskRunnerContext taskRunnerContext;
  private final QueryMasterProtocolService.Interface masterProxy;
  private final LocalDirAllocator lDirAllocator;
  private final QueryUnitAttemptId taskId;

  private final Path taskDir;
  private final QueryUnitRequest request;
  private TaskAttemptContext context;
  private List<Fetcher> fetcherRunners;
  private LogicalNode plan;
  private final Map<String, TableDesc> descs = Maps.newHashMap();
  private PhysicalExec executor;
  private boolean interQuery;
  private boolean killed = false;
  private boolean aborted = false;
  private boolean stopped = false;
  private final Reporter reporter;
  private Path inputTableBaseDir;

  private static int completedTasksNum = 0;
  private static int succeededTasksNum = 0;
  private static int killedTasksNum = 0;
  private static int failedTasksNum = 0;

  private long startTime;
  private long finishTime;

  private final TableStats inputStats;

  // TODO - to be refactored
  private ShuffleType shuffleType = null;
  private Schema finalSchema = null;
  private TupleComparator sortComp = null;
  private ClientSocketChannelFactory channelFactory = null;

  static final String OUTPUT_FILE_PREFIX="part-";
  static final ThreadLocal<NumberFormat> OUTPUT_FILE_FORMAT_SUBQUERY =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };
  static final ThreadLocal<NumberFormat> OUTPUT_FILE_FORMAT_TASK =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };

  public Task(QueryUnitAttemptId taskId,
              final TaskRunner.TaskRunnerContext worker,
              final QueryMasterProtocolService.Interface masterProxy,
              final QueryUnitRequest request) throws IOException {
    this.request = request;
    this.taskId = taskId;

    this.systemConf = worker.getConf();
    this.queryContext = request.getQueryContext();
    this.taskRunnerContext = worker;
    this.masterProxy = masterProxy;
    this.localFS = worker.getLocalFS();
    this.lDirAllocator = worker.getLocalDirAllocator();
    this.taskDir = StorageUtil.concatPath(taskRunnerContext.getBaseDir(),
        taskId.getQueryUnitId().getId() + "_" + taskId.getId());

    this.context = new TaskAttemptContext(systemConf, taskId,
        request.getFragments().toArray(new FragmentProto[request.getFragments().size()]), taskDir);
    this.context.setDataChannel(request.getDataChannel());
    this.context.setEnforcer(request.getEnforcer());
    this.inputStats = new TableStats();

    this.reporter = new Reporter(taskId, masterProxy);
    this.reporter.startCommunicationThread();

    plan = CoreGsonHelper.fromJson(request.getSerializedData(), LogicalNode.class);
    LogicalNode [] scanNode = PlannerUtil.findAllNodes(plan, NodeType.SCAN);
    for (LogicalNode node : scanNode) {
      ScanNode scan = (ScanNode)node;
      descs.put(scan.getCanonicalName(), scan.getTableDesc());
    }

    interQuery = request.getProto().getInterQuery();
    if (interQuery) {
      context.setInterQuery();
      this.shuffleType = context.getDataChannel().getShuffleType();

      if (shuffleType == ShuffleType.RANGE_SHUFFLE) {
        SortNode sortNode = PlannerUtil.findTopNode(plan, NodeType.SORT);
        this.finalSchema = PlannerUtil.sortSpecsToSchema(sortNode.getSortKeys());
        this.sortComp = new TupleComparator(finalSchema, sortNode.getSortKeys());
      }
    } else {
      // The final result of a task will be written in a file named part-ss-nnnnnnn,
      // where ss is the subquery id associated with this task, and nnnnnn is the task id.
      Path outFilePath = StorageUtil.concatPath(queryContext.getStagingDir(), TajoConstants.RESULT_DIR_NAME,
          OUTPUT_FILE_PREFIX +
          OUTPUT_FILE_FORMAT_SUBQUERY.get().format(taskId.getQueryUnitId().getExecutionBlockId().getId()) + "-" +
          OUTPUT_FILE_FORMAT_TASK.get().format(taskId.getQueryUnitId().getId()));
      LOG.info("Output File Path: " + outFilePath);
      context.setOutputPath(outFilePath);
    }

    context.setState(TaskAttemptState.TA_PENDING);
    LOG.info("==================================");
    LOG.info("* Subquery " + request.getId() + " is initialized");
    LOG.info("* InterQuery: " + interQuery
        + (interQuery ? ", Use " + this.shuffleType + " shuffle":""));

    LOG.info("* Fragments (num: " + request.getFragments().size() + ")");
    LOG.info("* Fetches (total:" + request.getFetches().size() + ") :");
    for (Fetch f : request.getFetches()) {
      LOG.info("Table Id: " + f.getName() + ", url: " + f.getUrls());
    }
    LOG.info("* Local task dir: " + taskDir);
    if(LOG.isDebugEnabled()) {
      LOG.debug("* plan:\n");
      LOG.debug(plan.toString());
    }
    LOG.info("==================================");
  }

  public void init() throws IOException {
    // initialize a task temporal dir
    localFS.mkdirs(taskDir);

    if (request.getFetches().size() > 0) {
      inputTableBaseDir = localFS.makeQualified(
          lDirAllocator.getLocalPathForWrite(
              getTaskAttemptDir(context.getTaskId()).toString(), systemConf));
      localFS.mkdirs(inputTableBaseDir);
      Path tableDir;
      for (String inputTable : context.getInputTables()) {
        tableDir = new Path(inputTableBaseDir, inputTable);
        if (!localFS.exists(tableDir)) {
          LOG.info("the directory is created  " + tableDir.toUri());
          localFS.mkdirs(tableDir);
        }
      }
    }
    // for localizing the intermediate data
    localize(request);
  }

  public QueryUnitAttemptId getTaskId() {
    return taskId;
  }

  public static Log getLog() {
    return LOG;
  }

  public void localize(QueryUnitRequest request) throws IOException {
    fetcherRunners = getFetchRunners(context, request.getFetches());
  }

  public QueryUnitAttemptId getId() {
    return context.getTaskId();
  }

  public TaskAttemptState getStatus() {
    return context.getState();
  }

  public String toString() {
    return "queryId: " + this.getId() + " status: " + this.getStatus();
  }

  public void setState(TaskAttemptState status) {
    context.setState(status);
  }

  public TaskAttemptContext getContext() {
    return context;
  }

  public boolean hasFetchPhase() {
    return fetcherRunners.size() > 0;
  }

  public void fetch() {
    for (Fetcher f : fetcherRunners) {
      taskRunnerContext.getFetchLauncher().submit(new FetchRunner(context, f));
    }
  }

  public void kill() {
    killed = true;
    context.stop();
    context.setState(TaskAttemptState.TA_KILLED);
    releaseChannelFactory();
  }

  public void abort() {
    aborted = true;
    context.stop();
    releaseChannelFactory();
  }

  public void cleanUp() {
    // remove itself from worker
    if (context.getState() == TaskAttemptState.TA_SUCCEEDED) {
      try {
        localFS.delete(context.getWorkDir(), true);
        synchronized (taskRunnerContext.getTasks()) {
          taskRunnerContext.getTasks().remove(this.getId());
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    } else {
      LOG.error("QueryUnitAttemptId: " + context.getTaskId() + " status: " + context.getState());
    }
  }

  public TaskStatusProto getReport() {
    TaskStatusProto.Builder builder = TaskStatusProto.newBuilder();
    builder.setWorkerName(taskRunnerContext.getNodeId());
    builder.setId(context.getTaskId().getProto())
        .setProgress(context.getProgress())
        .setState(context.getState());

    builder.setInputStats(reloadInputStats());

    if (context.getResultStats() != null) {
      builder.setResultStats(context.getResultStats().getProto());
    }
    return builder.build();
  }

  private CatalogProtos.TableStatsProto reloadInputStats() {
    synchronized(inputStats) {
      if (this.executor == null) {
        return inputStats.getProto();
      }

      TableStats executorInputStats = this.executor.getInputStats();

      if (executorInputStats != null) {
        inputStats.setValues(executorInputStats);
      }
      return inputStats.getProto();
    }
  }

  private TaskCompletionReport getTaskCompletionReport() {
    TaskCompletionReport.Builder builder = TaskCompletionReport.newBuilder();
    builder.setId(context.getTaskId().getProto());

    builder.setInputStats(reloadInputStats());

    if (context.hasResultStats()) {
      builder.setResultStats(context.getResultStats().getProto());
    } else {
      builder.setResultStats(new TableStats().getProto());
    }

    Iterator<Entry<Integer,String>> it = context.getShuffleFileOutputs();
    if (it.hasNext()) {
      do {
        Entry<Integer,String> entry = it.next();
        ShuffleFileOutput.Builder part = ShuffleFileOutput.newBuilder();
        part.setPartId(entry.getKey());
        builder.addShuffleFileOutputs(part.build());
      } while (it.hasNext());
    }

    return builder.build();
  }

  private void waitForFetch() throws InterruptedException, IOException {
    context.getFetchLatch().await();
    LOG.info(context.getTaskId() + " All fetches are done!");
    Collection<String> inputs = Lists.newArrayList(context.getInputTables());
    for (String inputTable: inputs) {
      File tableDir = new File(context.getFetchIn(), inputTable);
      FileFragment[] frags = localizeFetchedData(tableDir, inputTable, descs.get(inputTable).getMeta());
      context.updateAssignedFragments(inputTable, frags);
    }
    releaseChannelFactory();
  }

  public void run() {
    startTime = System.currentTimeMillis();
    String errorMessage = null;
    try {
      context.setState(TaskAttemptState.TA_RUNNING);

      if (context.hasFetchPhase()) {
        // If the fetch is still in progress, the query unit must wait for
        // complete.
        waitForFetch();
        context.setFetcherProgress(FETCHER_PROGRESS);
        context.setProgress(FETCHER_PROGRESS);
      }

      if (context.getFragmentSize() > 0) {
        this.executor = taskRunnerContext.getTQueryEngine().
            createPlan(context, plan);
        this.executor.init();
        while(!killed && executor.next() != null) {
        }
        this.executor.close();
        reloadInputStats();
        this.executor = null;
      }
    } catch (Exception e) {
      // errorMessage will be sent to master.
      errorMessage = ExceptionUtils.getStackTrace(e);
      LOG.error(errorMessage);
      aborted = true;
    } finally {
      context.setProgress(1.0f);
      stopped = true;
      completedTasksNum++;

      if (killed || aborted) {
        context.setExecutorProgress(0.0f);
        context.setProgress(0.0f);
        if(killed) {
          context.setState(TaskAttemptState.TA_KILLED);
          masterProxy.statusUpdate(null, getReport(), NullCallback.get());
          killedTasksNum++;
        } else {
          context.setState(TaskAttemptState.TA_FAILED);
          TaskFatalErrorReport.Builder errorBuilder =
              TaskFatalErrorReport.newBuilder()
                  .setId(getId().getProto());
          if (errorMessage != null) {
            errorBuilder.setErrorMessage(errorMessage);
          }

          masterProxy.fatalError(null, errorBuilder.build(), NullCallback.get());
          failedTasksNum++;
        }

        // stopping the status report
        try {
          reporter.stopCommunicationThread();
        } catch (InterruptedException e) {
          LOG.warn(e);
        }

      } else {
        // if successful
        context.setProgress(1.0f);
        context.setState(TaskAttemptState.TA_SUCCEEDED);

        // stopping the status report
        try {
          reporter.stopCommunicationThread();
        } catch (InterruptedException e) {
          LOG.warn(e);
        }

        TaskCompletionReport report = getTaskCompletionReport();
        masterProxy.done(null, report, NullCallback.get());
        succeededTasksNum++;
      }

      finishTime = System.currentTimeMillis();

      cleanupTask();
      LOG.info("Task Counter - total:" + completedTasksNum + ", succeeded: " + succeededTasksNum
          + ", killed: " + killedTasksNum + ", failed: " + failedTasksNum);
    }
  }

  public void cleanupTask() {
    taskRunnerContext.addTaskHistory(getId(), getTaskHistory());
    taskRunnerContext.getTasks().remove(getId());
    taskRunnerContext = null;

    fetcherRunners.clear();
    executor = null;
    plan = null;
    context = null;
    releaseChannelFactory();
  }

  public TaskHistory getTaskHistory() {
    TaskHistory taskHistory = new TaskHistory();
    taskHistory.setStartTime(startTime);
    taskHistory.setFinishTime(finishTime);
    if (context.getOutputPath() != null) {
      taskHistory.setOutputPath(context.getOutputPath().toString());
    }

    if (context.getWorkDir() != null) {
      taskHistory.setWorkingPath(context.getWorkDir().toString());
    }

    try {
      taskHistory.setStatus(getStatus().toString());
      taskHistory.setProgress(context.getProgress());

      taskHistory.setInputStats(new TableStats(reloadInputStats()));
      if (context.getResultStats() != null) {
        taskHistory.setOutputStats((TableStats)context.getResultStats().clone());
      }

      if (hasFetchPhase()) {
        Map<URI, TaskHistory.FetcherHistory> fetcherHistories = new HashMap<URI, TaskHistory.FetcherHistory>();

        for(Fetcher eachFetcher: fetcherRunners) {
          TaskHistory.FetcherHistory fetcherHistory = new TaskHistory.FetcherHistory();
          fetcherHistory.setStartTime(eachFetcher.getStartTime());
          fetcherHistory.setFinishTime(eachFetcher.getFinishTime());
          fetcherHistory.setStatus(eachFetcher.getStatus());
          fetcherHistory.setUri(eachFetcher.getURI().toString());
          fetcherHistory.setFileLen(eachFetcher.getFileLen());
          fetcherHistory.setMessageReceiveCount(eachFetcher.getMessageReceiveCount());

          fetcherHistories.put(eachFetcher.getURI(), fetcherHistory);
        }

        taskHistory.setFetchers(fetcherHistories);
      }
    } catch (Exception e) {
      taskHistory.setStatus(StringUtils.stringifyException(e));
      e.printStackTrace();
    }

    return taskHistory;
  }

  public int hashCode() {
    return context.hashCode();
  }

  public boolean equals(Object obj) {
    if (obj instanceof Task) {
      Task other = (Task) obj;
      return this.context.equals(other.context);
    }
    return false;
  }

  private FileFragment[] localizeFetchedData(File file, String name, TableMeta meta)
      throws IOException {
    Configuration c = new Configuration(systemConf);
    c.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "file:///");
    FileSystem fs = FileSystem.get(c);
    Path tablePath = new Path(file.getAbsolutePath());

    List<FileFragment> listTablets = new ArrayList<FileFragment>();
    FileFragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus f : fileLists) {
      if (f.getLen() == 0) {
        continue;
      }
      tablet = new FileFragment(name, f.getPath(), 0l, f.getLen());
      listTablets.add(tablet);
    }

    FileFragment[] tablets = new FileFragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }

  private class FetchRunner implements Runnable {
    private final TaskAttemptContext ctx;
    private final Fetcher fetcher;

    public FetchRunner(TaskAttemptContext ctx, Fetcher fetcher) {
      this.ctx = ctx;
      this.fetcher = fetcher;
    }

    @Override
    public void run() {
      int retryNum = 0;
      int maxRetryNum = 5;
      int retryWaitTime = 1000;

      try { // for releasing fetch latch
        while(retryNum < maxRetryNum) {
          if (retryNum > 0) {
            try {
              Thread.sleep(retryWaitTime);
            } catch (InterruptedException e) {
              LOG.error(e);
            }
            LOG.warn("Retry on the fetch: " + fetcher.getURI() + " (" + retryNum + ")");
          }
          try {
            File fetched = fetcher.get();
            if (fetched != null) {
              break;
            }
          } catch (IOException e) {
            LOG.error("Fetch failed: " + fetcher.getURI(), e);
          }
          retryNum++;
        }
      } finally {
        fetcherFinished(ctx);
      }

      if (retryNum == maxRetryNum) {
        LOG.error("ERROR: the maximum retry (" + retryNum + ") on the fetch exceeded (" + fetcher.getURI() + ")");
      }
    }
  }

  @VisibleForTesting
  public static float adjustFetchProcess(int totalFetcher, int remainFetcher) {
    return ((float)(totalFetcher - remainFetcher)) / (float)totalFetcher * FETCHER_PROGRESS;
  }

  private synchronized void fetcherFinished(TaskAttemptContext ctx) {
    int fetcherSize = fetcherRunners.size();
    if(fetcherSize == 0) {
      return;
    }
    try {
      int numRunningFetcher = (int)(ctx.getFetchLatch().getCount()) - 1;

      if (numRunningFetcher == 0) {
        context.setProgress(FETCHER_PROGRESS);
      } else {
        context.setProgress(adjustFetchProcess(fetcherSize, numRunningFetcher));
      }
    } finally {
      ctx.getFetchLatch().countDown();
    }
  }

  private void releaseChannelFactory(){
    if(channelFactory != null) {
      channelFactory.shutdown();
      channelFactory.releaseExternalResources();
      channelFactory = null;
    }
  }

  private List<Fetcher> getFetchRunners(TaskAttemptContext ctx,
                                        List<Fetch> fetches) throws IOException {

    if (fetches.size() > 0) {

      releaseChannelFactory();


      int workerNum = ctx.getConf().getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM);
      channelFactory = RpcChannelFactory.createClientChannelFactory("Fetcher", workerNum);
      Path inputDir = lDirAllocator.
          getLocalPathToRead(
              getTaskAttemptDir(ctx.getTaskId()).toString(), systemConf);
      File storeDir;

      int i = 0;
      File storeFile;
      List<Fetcher> runnerList = Lists.newArrayList();
      for (Fetch f : fetches) {
        storeDir = new File(inputDir.toString(), f.getName());
        if (!storeDir.exists()) {
          storeDir.mkdirs();
        }
        storeFile = new File(storeDir, "in_" + i);
        Fetcher fetcher = new Fetcher(URI.create(f.getUrls()), storeFile, channelFactory);
        runnerList.add(fetcher);
        i++;
      }
      ctx.addFetchPhase(runnerList.size(), new File(inputDir.toString()));
      return runnerList;
    } else {
      return Lists.newArrayList();
    }
  }

  protected class Reporter {
    private QueryMasterProtocolService.Interface masterStub;
    private Thread pingThread;
    private AtomicBoolean stop = new AtomicBoolean(false);
    private static final int PROGRESS_INTERVAL = 3000;
    private QueryUnitAttemptId taskId;

    public Reporter(QueryUnitAttemptId taskId, QueryMasterProtocolService.Interface masterStub) {
      this.taskId = taskId;
      this.masterStub = masterStub;
    }

    Runnable createReporterThread() {

      return new Runnable() {
        final int MAX_RETRIES = 3;
        int remainingRetries = MAX_RETRIES;
        @Override
        public void run() {
          while (!stop.get() && !stopped) {
            try {
              if(executor != null && context.getProgress() < 1.0f) {
                float progress = executor.getProgress();
                context.setExecutorProgress(progress);
              }
            } catch (Throwable t) {
              LOG.error("Get progress error: " + t.getMessage(), t);
            }

            try {
              if (context.isPorgressChanged()) {
                masterStub.statusUpdate(null, getReport(), NullCallback.get());
              } else {
                masterStub.ping(null, taskId.getProto(), NullCallback.get());
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
              if (remainingRetries > 0) {
                synchronized (pingThread) {
                  try {
                    pingThread.wait(PROGRESS_INTERVAL);
                  } catch (InterruptedException e) {
                  }
                }
              }
            }
          }
        }
      };
    }

    public void startCommunicationThread() {
      if (pingThread == null) {
        pingThread = new Thread(createReporterThread());
        pingThread.setName("communication thread");
        pingThread.start();
      }
    }

    public void stopCommunicationThread() throws InterruptedException {
      if(stop.getAndSet(true)){
        return;
      }

      if (pingThread != null) {
        // Intent of the lock is to not send an interupt in the middle of an
        // umbilical.ping or umbilical.statusUpdate
        synchronized(pingThread) {
          //Interrupt if sleeping. Otherwise wait for the RPC call to return.
          pingThread.notifyAll();
        }
      }
    }
  }

  public static final String FILECACHE = "filecache";
  public static final String APPCACHE = "appcache";
  public static final String USERCACHE = "usercache";

  String fileCache;
  public String getFileCacheDir() {
    fileCache = USERCACHE + "/" + "hyunsik" + "/" + APPCACHE + "/" +
        ConverterUtils.toString(ApplicationIdUtils.queryIdToAppId(taskId.getQueryUnitId().getExecutionBlockId().getQueryId())) +
        "/" + "output";
    return fileCache;
  }

  public static Path getTaskAttemptDir(QueryUnitAttemptId quid) {
    Path workDir =
        StorageUtil.concatPath(
            quid.getQueryUnitId().getExecutionBlockId().getQueryId().toString(),
            "in",
            quid.getQueryUnitId().getExecutionBlockId().toString(),
            String.valueOf(quid.getQueryUnitId().getId()),
            String.valueOf(quid.getId()));
    return workDir;
  }
}
