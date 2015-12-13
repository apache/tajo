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
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.physical.PhysicalExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.engine.query.TaskRequest;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.plan.function.python.TajoScriptEngine;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.logical.SortNode;
import org.apache.tajo.plan.serder.LogicalNodeDeserializer;
import org.apache.tajo.plan.serder.PlanProto.EnforceProperty;
import org.apache.tajo.plan.serder.PlanProto.EnforceProperty.EnforceType;
import org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.NetUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import static org.apache.tajo.ResourceProtos.*;

public class TaskImpl implements Task {
  private static final Log LOG = LogFactory.getLog(TaskImpl.class);
  private static final float FETCHER_PROGRESS = 0.5f;

  private final TajoConf systemConf;
  private final QueryContext queryContext;
  private final ExecutionBlockContext executionBlockContext;
  private final TaskRequest request;
  private final Map<String, TableDesc> descs;
  private final TableStats inputStats;
  private final Path taskDir;

  private final TaskAttemptContext context;
  private List<Fetcher> fetcherRunners;
  private LogicalNode plan;
  private PhysicalExec executor;

  private boolean interQuery;
  private Path inputTableBaseDir;

  private long startTime;
  private long endTime;

  private List<FileChunk> localChunks;
  // TODO - to be refactored
  private ShuffleType shuffleType = null;
  private Schema finalSchema = null;

  private TupleComparator sortComp = null;

  public TaskImpl(final TaskRequest request,
                  final ExecutionBlockContext executionBlockContext) throws IOException {

    this.request = request;
    this.executionBlockContext = executionBlockContext;
    this.systemConf = executionBlockContext.getConf();
    this.queryContext = request.getQueryContext(systemConf);
    this.inputStats = new TableStats();
    this.fetcherRunners = Lists.newArrayList();
    this.descs = Maps.newHashMap();

    Path baseDirPath = executionBlockContext.createBaseDir();

    if(LOG.isDebugEnabled()) LOG.debug("Task basedir is created (" + baseDirPath +")");

    TaskAttemptId taskAttemptId = request.getId();

    this.taskDir = StorageUtil.concatPath(baseDirPath,
        taskAttemptId.getTaskId().getId() + "_" + taskAttemptId.getId());
    this.context = new TaskAttemptContext(queryContext, executionBlockContext, taskAttemptId,
        request.getFragments().toArray(new FragmentProto[request.getFragments().size()]), taskDir);
    this.context.setDataChannel(request.getDataChannel());
    this.context.setEnforcer(request.getEnforcer());
    this.context.setState(TaskAttemptState.TA_PENDING);
  }

  public void initPlan() throws IOException {
    plan = LogicalNodeDeserializer.deserialize(queryContext, context.getEvalContext(), request.getPlan());
    updateDescsForScanNodes(NodeType.SCAN);
    updateDescsForScanNodes(NodeType.PARTITIONS_SCAN);
    updateDescsForScanNodes(NodeType.INDEX_SCAN);

    interQuery = request.getProto().getInterQuery();
    if (interQuery) {
      context.setInterQuery();
      this.shuffleType = context.getDataChannel().getShuffleType();

      if (shuffleType == ShuffleType.RANGE_SHUFFLE) {
        SortNode sortNode = PlannerUtil.findTopNode(plan, NodeType.SORT);
        this.finalSchema = PlannerUtil.sortSpecsToSchema(sortNode.getSortKeys());
        this.sortComp = new BaseTupleComparator(finalSchema, sortNode.getSortKeys());
      }
    } else {
      Path outFilePath = ((FileTablespace) TablespaceManager.get(queryContext.getStagingDir().toUri()))
          .getAppenderFilePath(getId(), queryContext.getStagingDir());
      LOG.info("Output File Path: " + outFilePath);
      context.setOutputPath(outFilePath);
    }

    this.localChunks = Collections.synchronizedList(new ArrayList<>());

    LOG.info(String.format("* Task %s is initialized. InterQuery: %b, Shuffle: %s, Fragments: %d, Fetches:%d, " +
        "Local dir: %s", request.getId(), interQuery, shuffleType, request.getFragments().size(),
        request.getFetches().size(), taskDir));

    if(LOG.isDebugEnabled()) {
      for (FetchImpl f : request.getFetches()) {
        LOG.debug("Table Id: " + f.getName() + ", Simple URIs: " + f.getSimpleURIs());
      }
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("* plan:\n");
      LOG.debug(plan.toString());
    }
  }

  private void updateDescsForScanNodes(NodeType nodeType) {
    assert nodeType == NodeType.SCAN || nodeType == NodeType.PARTITIONS_SCAN || nodeType == NodeType.INDEX_SCAN;
    LogicalNode[] scanNodes = PlannerUtil.findAllNodes(plan, nodeType);
    if (scanNodes != null) {
      for (LogicalNode node : scanNodes) {
        ScanNode scanNode = (ScanNode) node;
        descs.put(scanNode.getCanonicalName(), scanNode.getTableDesc());
      }
    }
  }

  private void startScriptExecutors() throws IOException {
    for (TajoScriptEngine executor : context.getEvalContext().getAllScriptEngines()) {
      executor.start(systemConf);
    }
  }

  private void stopScriptExecutors() {
    for (TajoScriptEngine executor : context.getEvalContext().getAllScriptEngines()) {
      executor.shutdown();
    }
  }

  @Override
  public void init() throws IOException {

    initPlan();
    startScriptExecutors();

    if (context.getState() == TaskAttemptState.TA_PENDING) {
      // initialize a task temporal dir
      FileSystem localFS = executionBlockContext.getLocalFS();
      localFS.mkdirs(taskDir);

      if (request.getFetches().size() > 0) {
        inputTableBaseDir = localFS.makeQualified(
            executionBlockContext.getLocalDirAllocator().getLocalPathForWrite(
                getTaskAttemptDir(context.getTaskId()).toString(), systemConf));
        localFS.mkdirs(inputTableBaseDir);
        Path tableDir;
        for (String inputTable : context.getInputTables()) {
          tableDir = new Path(inputTableBaseDir, inputTable);
          if (!localFS.exists(tableDir)) {
            localFS.mkdirs(tableDir);
            if(LOG.isDebugEnabled()) {
              LOG.debug("the directory is created  " + tableDir.toUri());
            }
          }
        }
      }
      // for localizing the intermediate data
      fetcherRunners.addAll(getFetchRunners(context, request.getFetches()));
    }
  }

  private TaskAttemptId getId() {
    return context.getTaskId();
  }

  public String toString() {
    return "TaskId: " + this.getId() + " Status: " + context.getState();
  }

  @Override
  public boolean isStopped() {
    return context.isStopped();
  }

  @Override
  public TaskAttemptContext getTaskContext() {
    return context;
  }

  @Override
  public ExecutionBlockContext getExecutionBlockContext() {
    return executionBlockContext;
  }

  @Override
  public boolean hasFetchPhase() {
    return fetcherRunners.size() > 0;
  }

  @Override
  public void fetch(ExecutorService fetcherExecutor) {
    for (Fetcher f : fetcherRunners) {
      fetcherExecutor.submit(new FetchRunner(context, f));
    }
  }

  @Override
  public void kill() {
    stopScriptExecutors();
    context.setState(TaskAttemptState.TA_KILLED);
    context.stop();
  }

  @Override
  public void abort() {
    stopScriptExecutors();
    context.setState(TaskAttemptState.TA_FAILED);
    context.stop();
  }

  @Override
  public TaskStatusProto getReport() {
    TaskStatusProto.Builder builder = TaskStatusProto.newBuilder();
    builder.setWorkerName(executionBlockContext.getWorkerContext().getConnectionInfo().getHostAndPeerRpcPort());
    builder.setId(context.getTaskId().getProto())
        .setProgress(context.getProgress())
        .setState(context.getState());

    builder.setInputStats(reloadInputStats());

    if (context.getResultStats() != null) {
      builder.setResultStats(context.getResultStats().getProto());
    }
    return builder.build();
  }

  @Override
  public boolean isProgressChanged() {
    return context.isProgressChanged();
  }

  @Override
  public void updateProgress() {
    if(context != null && context.isStopped()){
      return;
    }

    if (executor != null && context.getProgress() < 1.0f) {
      context.setExecutorProgress(executor.getProgress());
    }
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

    if (!context.getPartitions().isEmpty()) {
      builder.addAllPartitions(context.getPartitions());
    }

    Iterator<Entry<Integer, String>> it = context.getShuffleFileOutputs();
    if (it.hasNext()) {
      do {
        Entry<Integer, String> entry = it.next();
        ShuffleFileOutput.Builder part = ShuffleFileOutput.newBuilder();
        part.setPartId(entry.getKey());

        // Set output volume
        if (context.getPartitionOutputVolume() != null) {
          for (Entry<Integer, Long> e : context.getPartitionOutputVolume().entrySet()) {
            if (entry.getKey().equals(e.getKey())) {
              part.setVolume(e.getValue().longValue());
              break;
            }
          }
        }

        builder.addShuffleFileOutputs(part.build());
      } while (it.hasNext());
    }

    return builder.build();
  }

  private void waitForFetch() throws InterruptedException, IOException {
    context.getFetchLatch().await();
    LOG.info(context.getTaskId() + " All fetches are done!");
    Collection<String> inputs = Lists.newArrayList(context.getInputTables());

    // Get all broadcasted tables
    Set<String> broadcastTableNames = new HashSet<>();
    List<EnforceProperty> broadcasts = context.getEnforcer().getEnforceProperties(EnforceType.BROADCAST);
    if (broadcasts != null) {
      for (EnforceProperty eachBroadcast : broadcasts) {
        broadcastTableNames.add(eachBroadcast.getBroadcast().getTableName());
      }
    }

    // localize the fetched data and skip the broadcast table
    for (String inputTable: inputs) {
      if (broadcastTableNames.contains(inputTable)) {
        continue;
      }
      File tableDir = new File(context.getFetchIn(), inputTable);
      FileFragment[] frags = localizeFetchedData(tableDir, inputTable, descs.get(inputTable).getMeta());
      context.updateAssignedFragments(inputTable, frags);
    }
  }

  @Override
  public void run() throws Exception {
    startTime = System.currentTimeMillis();
    Throwable error = null;

    try {
      if(!context.isStopped()) {
        context.setState(TajoProtos.TaskAttemptState.TA_RUNNING);
        if (context.hasFetchPhase()) {
          // If the fetch is still in progress, the query unit must wait for complete.
          waitForFetch();
          context.setFetcherProgress(FETCHER_PROGRESS);
          updateProgress();
        }

        this.executor = executionBlockContext.getTQueryEngine().createPlan(context, plan);
        this.executor.init();

        while(!context.isStopped() && executor.next() != null) {
        }
      }
    } catch (Throwable e) {
      error = e ;
      LOG.error(e.getMessage(), e);
      stopScriptExecutors();
      context.stop();
    } finally {
      if (executor != null) {
        try {
          executor.close();
          reloadInputStats();
        } catch (IOException e) {
          LOG.error(e, e);
        }
        this.executor = null;
      }

      executionBlockContext.completedTasksNum.incrementAndGet();
      context.getHashShuffleAppenderManager().finalizeTask(getId());

      QueryMasterProtocol.QueryMasterProtocolService.Interface queryMasterStub = executionBlockContext.getStub();
      if (context.isStopped()) {
        context.setExecutorProgress(0.0f);

        if (context.getState() == TaskAttemptState.TA_KILLED) {
          queryMasterStub.statusUpdate(null, getReport(), NullCallback.get());
          executionBlockContext.killedTasksNum.incrementAndGet();
        } else {
          context.setState(TaskAttemptState.TA_FAILED);
          TaskFatalErrorReport.Builder errorBuilder =
              TaskFatalErrorReport.newBuilder()
                  .setId(getId().getProto());
          if (error != null) {
            if (error.getMessage() == null) {
              errorBuilder.setErrorMessage(error.getClass().getCanonicalName());
            } else {
              errorBuilder.setErrorMessage(error.getMessage());
            }
            errorBuilder.setErrorTrace(ExceptionUtils.getStackTrace(error));
          }

          queryMasterStub.fatalError(null, errorBuilder.build(), NullCallback.get());
          executionBlockContext.failedTasksNum.incrementAndGet();
        }
      } else {
        // if successful
        context.stop();
        context.setProgress(1.0f);
        context.setState(TaskAttemptState.TA_SUCCEEDED);
        executionBlockContext.succeededTasksNum.incrementAndGet();

        TaskCompletionReport report = getTaskCompletionReport();
        queryMasterStub.done(null, report, NullCallback.get());
      }
      endTime = System.currentTimeMillis();
      LOG.info(String.format("%s is complete. %d ms elapsed, final state:%s",
          context.getTaskId(), endTime - startTime, context.getState()));
    }
  }

  @Override
  public void cleanup() {
    // history store in memory while running stage
    TaskHistory taskHistory = createTaskHistory();
    executionBlockContext.addTaskHistory(getId().getTaskId(), taskHistory);
    executionBlockContext.getTasks().remove(getId());

    fetcherRunners.clear();
    fetcherRunners = null;
    try {
      if(executor != null) {
        executor.close();
        executor = null;
      }
    } catch (IOException e) {
      LOG.fatal(e.getMessage(), e);
    }

    executionBlockContext.getWorkerContext().getTaskHistoryWriter().appendHistory(taskHistory);
    stopScriptExecutors();
  }

  @Override
  public TaskHistory createTaskHistory() {
    TaskHistory taskHistory = null;
    try {
      taskHistory = new TaskHistory(context.getTaskId(), context.getState(), context.getProgress(),
          startTime, endTime, reloadInputStats());

      if (context.getOutputPath() != null) {
        taskHistory.setOutputPath(context.getOutputPath().toString());
      }

      if (context.getWorkDir() != null) {
        taskHistory.setWorkingPath(context.getWorkDir().toString());
      }

      if (context.getResultStats() != null) {
        taskHistory.setOutputStats(context.getResultStats().getProto());
      }

      if (hasFetchPhase()) {
        taskHistory.setTotalFetchCount(fetcherRunners.size());
        int i = 0;
        FetcherHistoryProto.Builder builder = FetcherHistoryProto.newBuilder();
        for (Fetcher fetcher : fetcherRunners) {
          builder.setStartTime(fetcher.getStartTime());
          builder.setFinishTime(fetcher.getFinishTime());
          builder.setFileLength(fetcher.getFileLen());
          builder.setMessageReceivedCount(fetcher.getMessageReceiveCount());
          builder.setState(fetcher.getState());
          taskHistory.addFetcherHistory(builder.build());
          if (fetcher.getState() == TajoProtos.FetcherState.FETCH_FINISHED) i++;
        }
        taskHistory.setFinishedFetchCount(i);
      }
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }

    return taskHistory;
  }

  public List<Fetcher> getFetchers() {
    return fetcherRunners;
  }

  public int hashCode() {
    return context.hashCode();
  }

  public boolean equals(Object obj) {
    if (obj instanceof TaskImpl) {
      TaskImpl other = (TaskImpl) obj;
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

    List<FileFragment> listTablets = new ArrayList<>();
    FileFragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus f : fileLists) {
      if (f.getLen() == 0) {
        continue;
      }
      tablet = new FileFragment(name, fs.makeQualified(f.getPath()), 0l, f.getLen());
      listTablets.add(tablet);
    }

    // Special treatment for locally pseudo fetched chunks
    synchronized (localChunks) {
      for (FileChunk chunk : localChunks) {
        if (name.equals(chunk.getEbId())) {
          tablet = new FileFragment(name, fs.makeQualified(new Path(chunk.getFile().getPath())), chunk.startOffset(), chunk.length());
          listTablets.add(tablet);
        }
      }
    }

    FileFragment[] tablets = new FileFragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }

  private class FetchRunner implements Runnable {
    private final TaskAttemptContext ctx;
    private final Fetcher fetcher;
    private int maxRetryNum;

    public FetchRunner(TaskAttemptContext ctx, Fetcher fetcher) {
      this.ctx = ctx;
      this.fetcher = fetcher;
      this.maxRetryNum = systemConf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_READ_RETRY_MAX_NUM);
    }

    @Override
    public void run() {
      int retryNum = 0;
      int retryWaitTime = 1000; //sec

      try { // for releasing fetch latch
        while(!context.isStopped() && retryNum < maxRetryNum) {
          if (retryNum > 0) {
            try {
              Thread.sleep(retryWaitTime);
              retryWaitTime = Math.min(10 * 1000, retryWaitTime * 2);  // max 10 seconds
            } catch (InterruptedException e) {
              LOG.error(e);
            }
            LOG.warn("Retry on the fetch: " + fetcher.getURI() + " (" + retryNum + ")");
          }
          try {
            FileChunk fetched = fetcher.get();
            if (fetcher.getState() == TajoProtos.FetcherState.FETCH_FINISHED && fetched != null
                && fetched.getFile() != null) {
              if (fetched.fromRemote() == false) {
                localChunks.add(fetched);
              }
              break;
            }
          } catch (Throwable e) {
            LOG.error("Fetch failed: " + fetcher.getURI(), e);
          }
          retryNum++;
        }
      } finally {
        if(fetcher.getState() == TajoProtos.FetcherState.FETCH_FINISHED){
          fetcherFinished(ctx);
        } else {
          if (retryNum == maxRetryNum) {
            LOG.error("ERROR: the maximum retry (" + retryNum + ") on the fetch exceeded (" + fetcher.getURI() + ")");
          }
          stopScriptExecutors();
          context.stop(); // retry task
          ctx.getFetchLatch().countDown();
        }
      }
    }
  }

  @VisibleForTesting
  public static float adjustFetchProcess(int totalFetcher, int remainFetcher) {
    if (totalFetcher > 0) {
      return ((totalFetcher - remainFetcher) / (float) totalFetcher) * FETCHER_PROGRESS;
    } else {
      return 0.0f;
    }
  }

  private synchronized void fetcherFinished(TaskAttemptContext ctx) {
    int fetcherSize = fetcherRunners.size();
    if(fetcherSize == 0) {
      return;
    }

    ctx.getFetchLatch().countDown();

    int remainFetcher = (int) ctx.getFetchLatch().getCount();
    if (remainFetcher == 0) {
      context.setFetcherProgress(FETCHER_PROGRESS);
    } else {
      context.setFetcherProgress(adjustFetchProcess(fetcherSize, remainFetcher));
    }
  }

  private List<Fetcher> getFetchRunners(TaskAttemptContext ctx,
                                        List<FetchImpl> fetches) throws IOException {

    if (fetches.size() > 0) {
      Path inputDir = executionBlockContext.getLocalDirAllocator().
          getLocalPathToRead(getTaskAttemptDir(ctx.getTaskId()).toString(), systemConf);

      int i = 0;
      int localStoreChunkCount = 0;
      File storeDir;
      File defaultStoreFile;
      FileChunk storeChunk = null;
      List<Fetcher> runnerList = Lists.newArrayList();

      for (FetchImpl f : fetches) {
        storeDir = new File(inputDir.toString(), f.getName());
        if (!storeDir.exists()) {
          if (!storeDir.mkdirs()) throw new IOException("Failed to create " + storeDir);
        }

        for (URI uri : f.getURIs()) {
          defaultStoreFile = new File(storeDir, "in_" + i);
          InetAddress address = InetAddress.getByName(uri.getHost());

          WorkerConnectionInfo conn = executionBlockContext.getWorkerContext().getConnectionInfo();
          if (NetUtils.isLocalAddress(address) && conn.getPullServerPort() == uri.getPort()) {

            storeChunk = getLocalStoredFileChunk(uri, systemConf);

            // When a range request is out of range, storeChunk will be NULL. This case is normal state.
            // So, we should skip and don't need to create storeChunk.
            if (storeChunk == null || storeChunk.length() == 0) {
              continue;
            }

            if (storeChunk.getFile() != null && storeChunk.startOffset() > -1) {
              storeChunk.setFromRemote(false);
              localStoreChunkCount++;
            } else {
              storeChunk = new FileChunk(defaultStoreFile, 0, -1);
              storeChunk.setFromRemote(true);
            }
          } else {
            storeChunk = new FileChunk(defaultStoreFile, 0, -1);
            storeChunk.setFromRemote(true);
          }

          // If we decide that intermediate data should be really fetched from a remote host, storeChunk
          // represents a complete file. Otherwise, storeChunk may represent a complete file or only a part of it
          storeChunk.setEbId(f.getName());
          Fetcher fetcher = new Fetcher(systemConf, uri, storeChunk);
          runnerList.add(fetcher);
          i++;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Create a new Fetcher with storeChunk:" + storeChunk.toString());
          }
        }
      }
      ctx.addFetchPhase(runnerList.size(), new File(inputDir.toString()));
      LOG.info("Create shuffle Fetchers local:" + localStoreChunkCount +
          ", remote:" + (runnerList.size() - localStoreChunkCount));
      return runnerList;
    } else {
      return Lists.newArrayList();
    }
  }

  private FileChunk getLocalStoredFileChunk(URI fetchURI, TajoConf conf) throws IOException {
    // Parse the URI

    // Parsing the URL into key-values
    final Map<String, List<String>> params = TajoPullServerService.decodeParams(fetchURI.toString());

    String partId = params.get("p").get(0);
    String queryId = params.get("qid").get(0);
    String shuffleType = params.get("type").get(0);
    String sid =  params.get("sid").get(0);

    final List<String> taskIdList = params.get("ta");
    final List<String> offsetList = params.get("offset");
    final List<String> lengthList = params.get("length");

    long offset = (offsetList != null && !offsetList.isEmpty()) ? Long.parseLong(offsetList.get(0)) : -1L;
    long length = (lengthList != null && !lengthList.isEmpty()) ? Long.parseLong(lengthList.get(0)) : -1L;

    if (LOG.isDebugEnabled()) {
      LOG.debug("PullServer request param: shuffleType=" + shuffleType + ", sid=" + sid + ", partId=" + partId
          + ", taskIds=" + taskIdList);
    }

    // The working directory of Tajo worker for each query, including stage
    Path queryBaseDir = TajoPullServerService.getBaseOutputDir(queryId, sid);
    List<String> taskIds = TajoPullServerService.splitMaps(taskIdList);

    FileChunk chunk;
    // If the stage requires a range shuffle
    if (shuffleType.equals("r")) {

      Path outputPath = StorageUtil.concatPath(queryBaseDir, taskIds.get(0), "output");
      if (!executionBlockContext.getLocalDirAllocator().ifExists(outputPath.toString(), conf)) {
        LOG.warn("Range shuffle - file not exist. " + outputPath);
        return null;
      }
      Path path = executionBlockContext.getLocalFS().makeQualified(
	      executionBlockContext.getLocalDirAllocator().getLocalPathToRead(outputPath.toString(), conf));
      String startKey = params.get("start").get(0);
      String endKey = params.get("end").get(0);
      boolean last = params.get("final") != null;

      try {
        chunk = TajoPullServerService.getFileChunks(path, startKey, endKey, last);
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        return null;
      }

      // If the stage requires a hash shuffle or a scattered hash shuffle
    } else if (shuffleType.equals("h") || shuffleType.equals("s")) {
      int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
      Path partPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);

      if (!executionBlockContext.getLocalDirAllocator().ifExists(partPath.toString(), conf)) {
        LOG.warn("Hash shuffle or Scattered hash shuffle - file not exist: " + partPath);
        return null;
      }
      Path path = executionBlockContext.getLocalFS().makeQualified(
        executionBlockContext.getLocalDirAllocator().getLocalPathToRead(partPath.toString(), conf));
      File file = new File(path.toUri());
      long startPos = (offset >= 0 && length >= 0) ? offset : 0;
      long readLen = (offset >= 0 && length >= 0) ? length : file.length();

      if (startPos >= file.length()) {
        LOG.error("Start pos[" + startPos + "] great than file length [" + file.length() + "]");
        return null;
      }
      chunk = new FileChunk(file, startPos, readLen);

    } else {
      LOG.error("Unknown shuffle type");
      return null;
    }

    return chunk;
  }

  public static Path getTaskAttemptDir(TaskAttemptId quid) {
    Path workDir =
        StorageUtil.concatPath(ExecutionBlockContext.getBaseInputDir(quid.getTaskId().getExecutionBlockId()),
            String.valueOf(quid.getTaskId().getId()),
            String.valueOf(quid.getId()));
    return workDir;
  }
}
