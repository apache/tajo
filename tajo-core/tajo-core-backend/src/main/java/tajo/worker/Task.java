/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.worker;

import com.google.common.collect.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import tajo.QueryConf;
import tajo.QueryUnitAttemptId;
import tajo.TajoProtos.TaskAttemptState;
import tajo.TaskAttemptContext;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.catalog.statistics.TableStat;
import tajo.engine.MasterWorkerProtos.*;
import tajo.engine.exception.UnfinishedTaskException;
import tajo.engine.json.GsonCreator;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.SortNode;
import tajo.engine.planner.logical.StoreTableNode;
import tajo.engine.planner.physical.PhysicalExec;
import tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService.Interface;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.master.SubQuery.PARTITION_TYPE;
import tajo.rpc.NullCallback;
import tajo.storage.Fragment;
import tajo.storage.StorageUtil;
import tajo.storage.TupleComparator;
import tajo.worker.TaskRunner.WorkerContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

public class Task {
  private static final Log LOG = LogFactory.getLog(Task.class);

  private final QueryConf conf;
  private final FileSystem localFS;
  private final WorkerContext workerContext;
  private final Interface masterProxy;
  private final LocalDirAllocator lDirAllocator;
  private final QueryUnitAttemptId taskId;

  private final QueryUnitRequest request;
  private final TaskAttemptContext context;
  private List<Fetcher> fetcherRunners;
  private final LogicalNode plan;
  private PhysicalExec executor;
  private boolean interQuery;
  private boolean killed = false;
  private boolean aborted = false;
  private boolean stopped = false;
  private float progress = 0;
  private final Reporter reporter;
  private Path inputTableBaseDir;

  private static int completed = 0;
  private static int failed = 0;
  private static int succeeded = 0;

  /**
   * flag that indicates whether progress update needs to be sent to parent.
   * If true, it has been set. If false, it has been reset.
   * Using AtomicBoolean since we need an atomic read & reset method.
   */
  private AtomicBoolean progressFlag = new AtomicBoolean(false);

  // TODO - to be refactored
  private PARTITION_TYPE partitionType = null;
  private Schema finalSchema = null;
  private TupleComparator sortComp = null;

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
              final WorkerContext worker, final Interface masterProxy,
              final QueryUnitRequest request, Path taskDir) throws IOException {
    this.request = request;
    this.reporter = new Reporter(masterProxy);
    this.reporter.startCommunicationThread();

    this.taskId = request.getId();
    this.conf = worker.getConf();
    this.workerContext = worker;
    this.masterProxy = masterProxy;
    this.localFS = worker.getLocalFS();
    this.lDirAllocator = worker.getLocalDirAllocator();

    this.context = new TaskAttemptContext(conf, taskId,
        request.getFragments().toArray(new Fragment[request.getFragments().size()]),
        taskDir);
    plan = GsonCreator.getInstance().fromJson(request.getSerializedData(),
        LogicalNode.class);
    interQuery = request.getProto().getInterQuery();
    if (interQuery) {
      context.setInterQuery();
      StoreTableNode store = (StoreTableNode) plan;
      this.partitionType = store.getPartitionType();
      if (partitionType == PARTITION_TYPE.RANGE) {
        SortNode sortNode = (SortNode) store.getSubNode();
        this.finalSchema = PlannerUtil.sortSpecsToSchema(sortNode.getSortKeys());
        this.sortComp = new TupleComparator(finalSchema, sortNode.getSortKeys());
      }
    } else {
      Path outFilePath = new Path(conf.getOutputPath(),
          OUTPUT_FILE_FORMAT_SUBQUERY.get().format(taskId.getSubQueryId().getId()) +
          OUTPUT_FILE_PREFIX +
          OUTPUT_FILE_FORMAT_TASK.get().format(taskId.getQueryUnitId().getId()));
      LOG.info("Output File Path: " + outFilePath);
      context.setOutputPath(outFilePath);
    }

    context.setState(TaskAttemptState.TA_PENDING);
    LOG.info("==================================");
    LOG.info("* Subquery " + request.getId() + " is initialized");
    LOG.info("* InterQuery: " + interQuery
        + (interQuery ? ", Use " + this.partitionType  + " partitioning":""));

    LOG.info("* Fragments (num: " + request.getFragments().size() + ")");
    for (Fragment f: request.getFragments()) {
      LOG.info("==> Table Id:" + f.getId() + ", path:" + f.getPath() + "(" + f.getMeta().getStoreType() + "), " +
          "(start:" + f.getStartOffset() + ", length: " + f.getLength() + ")");
    }
    LOG.info("* Fetches (total:" + request.getFetches().size() + ") :");
    for (Fetch f : request.getFetches()) {
      LOG.info("==> Table Id: " + f.getName() + ", url: " + f.getUrls());
    }
    LOG.info("* Local task dir: " + taskDir);
    LOG.info("* plan:\n");
    LOG.info(plan.toString());
    LOG.info("==================================");
  }

  public void init() throws IOException {
    if (request.getFetches().size() > 0) {
      inputTableBaseDir = localFS.makeQualified(
          lDirAllocator.getLocalPathForWrite(
              getTaskAttemptDir(context.getTaskId()).toString() + "/in", conf));
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

  // getters and setters for flag
  void setProgressFlag() {
    progressFlag.set(true);
  }
  boolean resetProgressFlag() {
    return progressFlag.getAndSet(false);
  }
  boolean getProgressFlag() {
    return progressFlag.get();
  }

  public void localize(QueryUnitRequest request) throws IOException {
    fetcherRunners = getFetchRunners(context, request.getFetches());

    List<Fragment> cached = Lists.newArrayList();
    for (Fragment frag : request.getFragments()) {
      if (frag.isDistCached()) {
        cached.add(frag);
      }
    }

    if (cached.size() > 0) {
      Path inFile;

      int i = fetcherRunners.size();
      for (Fragment cache : cached) {
        inFile = new Path(inputTableBaseDir, "in_" + i);
        workerContext.getDefaultFS().copyToLocalFile(cache.getPath(), inFile);
        cache.setPath(inFile);
        i++;
      }
    }
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
    setProgressFlag();
  }

  public boolean hasFetchPhase() {
    return fetcherRunners.size() > 0;
  }

  public void fetch() {
    for (Fetcher f : fetcherRunners) {
      workerContext.getFetchLauncher().submit(new FetchRunner(context, f));
    }
  }

  public void kill() {
    killed = true;
    context.stop();
    context.setState(TaskAttemptState.TA_KILLED);
    setProgressFlag();
  }

  public void abort() {
    aborted = true;
    context.stop();
    context.setState(TaskAttemptState.TA_FAILED);
  }

  public void cleanUp() {
    // remove itself from worker
    // 끝난건지 확인
    if (context.getState() == TaskAttemptState.TA_SUCCEEDED) {
      try {
        // context.getWorkDir() 지우기
        localFS.delete(context.getWorkDir(), true);
        // tasks에서 자기 지우기
        synchronized (workerContext.getTasks()) {
          workerContext.getTasks().remove(this.getId());
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      LOG.error(new UnfinishedTaskException("QueryUnitAttemptId: "
          + context.getTaskId() + " status: " + context.getState()));
    }
  }

  public TaskStatusProto getReport() {
    TaskStatusProto.Builder builder = TaskStatusProto.newBuilder();
    builder.setWorkerName(workerContext.getNodeId());
    builder.setId(context.getTaskId().getProto())
        .setProgress(context.getProgress()).setState(context.getState());

    return builder.build();
  }

  private TaskCompletionReport getTaskCompletionReport() {
    TaskCompletionReport.Builder builder = TaskCompletionReport.newBuilder();
    builder.setId(context.getTaskId().getProto());

    if (context.hasResultStats()) {
      builder.setResultStats(context.getResultStats().getProto());
    } else {
      builder.setResultStats(new TableStat().getProto());
    }

    Iterator<Entry<Integer,String>> it = context.getRepartitions();
    if (it.hasNext()) {
      do {
        Entry<Integer,String> entry = it.next();
        Partition.Builder part = Partition.newBuilder();
        part.setPartitionKey(entry.getKey());
        if (partitionType == PARTITION_TYPE.HASH) {
//          part.setFileName(
//              dataServerURL + "/?qid=" + getId().toString() + "&fn=" +
//                  entry.getValue());
        } else if (partitionType == PARTITION_TYPE.LIST) {
//          part.setFileName(dataServerURL + "/?qid=" + getId().toString() +
//              "&fn=0");
        } else {
//          part.setFileName(dataServerURL + "/?qid=" + getId().toString());
        }
        builder.addPartitions(part.build());
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
      Fragment [] frags = localizeFetchedData(tableDir, inputTable,
          context.getTable(inputTable).getMeta());
      context.changeFragment(inputTable, frags);
    }
  }

  public void run() {

    String errorMessage = null;
    try {
      context.setState(TaskAttemptState.TA_RUNNING);
      setProgressFlag();

      if (context.hasFetchPhase()) {
        // If the fetch is still in progress, the query unit must wait for
        // complete.
        waitForFetch();
      }

      if (context.getFragmentSize() > 0) {
        this.executor = workerContext.getTQueryEngine().
            createPlan(context, plan);
        this.executor.init();
        while(executor.next() != null && !killed) {
          ++progress;
        }
        this.executor.close();
      }
    } catch (Exception e) {
      // errorMessage will be sent to master.
      errorMessage = ExceptionUtils.getStackTrace(e);
      LOG.error(errorMessage);
      aborted = true;

    } finally {
      setProgressFlag();
      stopped = true;
      completed++;

      if (killed || aborted) {
        context.setProgress(0.0f);

        TaskFatalErrorReport.Builder errorBuilder =
            TaskFatalErrorReport.newBuilder()
            .setId(getId().getProto());
        if (errorMessage != null) {
            errorBuilder.setErrorMessage(errorMessage);
        }

        // stopping the status report
        try {
          reporter.stopCommunicationThread();
        } catch (InterruptedException e) {
          LOG.warn(e);
        }

        masterProxy.fatalError(null, errorBuilder.build(), NullCallback.get());
        failed++;

      } else {
        // if successful
        context.setProgress(1.0f);

        // stopping the status report
        try {
          reporter.stopCommunicationThread();
        } catch (InterruptedException e) {
          LOG.warn(e);
        }

        TaskCompletionReport report = getTaskCompletionReport();
        masterProxy.done(null, report, NullCallback.get());
        succeeded++;
      }

      cleanupTask();
      LOG.info("Task Counter - total:" + completed + ", succeeded: " + succeeded
          + ", failed: " + failed);
    }
  }

  public void cleanupTask() {
    workerContext.getTasks().remove(getId());
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

  private Fragment[] localizeFetchedData(File file, String name, TableMeta meta)
      throws IOException {
    Configuration c = new Configuration(conf);
    c.set("fs.default.name", "file:///");
    FileSystem fs = FileSystem.get(c);
    Path tablePath = new Path(file.getAbsolutePath());

    List<Fragment> listTablets = new ArrayList<>();
    Fragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus f : fileLists) {
      if (f.getLen() == 0) {
        continue;
      }
      tablet = new Fragment(name, f.getPath(), meta, 0l, f.getLen(), null);
      listTablets.add(tablet);
    }

    Fragment[] tablets = new Fragment[listTablets.size()];
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
            LOG.info("Retry on the fetch: " + fetcher.getURI() + " (" + retryNum + ")");
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
        ctx.getFetchLatch().countDown();
      }

      if (retryNum == maxRetryNum) {
        LOG.error("ERROR: the maximum retry (" + retryNum + ") on the fetch exceeded (" + fetcher.getURI() + ")");
      }
    }
  }

  private List<Fetcher> getFetchRunners(TaskAttemptContext ctx,
                                        List<Fetch> fetches) throws IOException {

    if (fetches.size() > 0) {
      Path inputDir = lDirAllocator.
          getLocalPathToRead(
              getTaskAttemptDir(ctx.getTaskId()).toString() + "/in", conf);
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
        Fetcher fetcher = new Fetcher(URI.create(f.getUrls()), storeFile);
        runnerList.add(fetcher);
        i++;
      }
      ctx.addFetchPhase(runnerList.size(), new File(inputDir.toString()));
      return runnerList;
    } else {
      return Lists.newArrayList();
    }
  }

  protected class Reporter implements Runnable {
    private Interface masterStub;
    private Thread pingThread;
    private Object lock = new Object();
    private static final int PROGRESS_INTERVAL = 3000;

    public Reporter(Interface masterStub) {
      this.masterStub = masterStub;
    }

    @Override
    public void run() {
      final int MAX_RETRIES = 3;
      int remainingRetries = MAX_RETRIES;

      while (!stopped) {
        try {
          synchronized (lock) {
            if (stopped) {
              break;
            }
            lock.wait(PROGRESS_INTERVAL);
          }
          if (stopped) {
            break;
          }
          resetProgressFlag();

          if (getProgressFlag()) {
            resetProgressFlag();
            masterStub.statusUpdate(null, getReport(), NullCallback.get());
          } else {
            masterStub.ping(null, taskId.getProto(), NullCallback.get());
          }

        } catch (Throwable t) {

          LOG.info("Communication exception: " + StringUtils
              .stringifyException(t));
          remainingRetries -=1;
          if (remainingRetries == 0) {
            ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
            LOG.warn("Last retry, killing ");
            System.exit(65);
          }
        }
      }
    }

    public void startCommunicationThread() {
      if (pingThread == null) {
        pingThread = new Thread(this, "communication thread");
        pingThread.setDaemon(true);
        pingThread.start();
      }
    }

    public void stopCommunicationThread() throws InterruptedException {
      if (pingThread != null) {
        // Intent of the lock is to not send an interupt in the middle of an
        // umbilical.ping or umbilical.statusUpdate
        synchronized(lock) {
          //Interrupt if sleeping. Otherwise wait for the RPC call to return.
          lock.notify();
        }

        pingThread.interrupt();
        pingThread.join();
      }
    }
  }

  public static final String FILECACHE = "filecache";
  public static final String APPCACHE = "appcache";
  public static final String USERCACHE = "usercache";

  String fileCache;
  public String getFileCacheDir() {
    fileCache = USERCACHE + "/" + "hyunsik" + "/" + APPCACHE + "/" +
        ConverterUtils.toString(taskId.getQueryId().getApplicationId()) +
        "/" + "output";
    return fileCache;
  }

  public static Path getTaskAttemptDir(QueryUnitAttemptId quid) {
    Path workDir =
        StorageUtil.concatPath(
            quid.getSubQueryId().toString(),
            String.valueOf(quid.getQueryUnitId().getId()),
            String.valueOf(quid.getId()));
    return workDir;
  }
}
