/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
import tajo.QueryUnitAttemptId;
import tajo.TaskAttemptContext;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.engine.MasterWorkerProtos.Fetch;
import tajo.engine.MasterWorkerProtos.Partition;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.engine.exception.InternalException;
import tajo.engine.exception.UnfinishedTaskException;
import tajo.engine.json.GsonCreator;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.logical.*;
import tajo.engine.planner.physical.PhysicalExec;
import tajo.engine.planner.physical.TupleComparator;
import tajo.ipc.protocolrecords.Fragment;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.master.SubQuery;
import tajo.worker.Worker.WorkerContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

public class Task implements Runnable {
  private static final Log LOG = LogFactory.getLog(Task.class);

  private final TajoConf conf;
  private final FileSystem localFS;
  private final WorkerContext workerContext;
  private final LocalDirAllocator lDirAllocator;

  private final TaskAttemptContext context;
  private List<Fetcher> fetcherRunners;
  private final LogicalNode plan;
  private PhysicalExec executor;
  private boolean interQuery;
  private boolean killed = false;
  private boolean aborted = false;
  private boolean finished = false;
  private int progress = 0;

  // TODO - to be refactored
  private SubQuery.PARTITION_TYPE partitionType = null;
  private Schema finalSchema = null;
  private TupleComparator sortComp = null;

  public Task(WorkerContext worker,
              QueryUnitRequest request) throws IOException {

    this.conf = worker.getConf();
    this.workerContext = worker;
    this.localFS = worker.getLocalFS();
    this.lDirAllocator = worker.getLocalDirAllocator();

    Path taskAttemptPath = localFS.makeQualified(lDirAllocator.
        getLocalPathForWrite(request.getId().toString(), conf));
    File taskAttemptDir = new File(taskAttemptPath.toUri());
    taskAttemptDir.mkdirs();

    this.context = new TaskAttemptContext(conf, request.getId(),
        request.getFragments().toArray(new Fragment[request.getFragments().size()]),
        taskAttemptDir);
    plan = GsonCreator.getInstance().fromJson(request.getSerializedData(),
        LogicalNode.class);
    interQuery = request.getProto().getInterQuery();
    if (interQuery) {
      context.setInterQuery();
      StoreTableNode store = (StoreTableNode) plan;
      this.partitionType = store.getPartitionType();
      if (store.getSubNode().getType() == ExprType.SORT) {
        SortNode sortNode = (SortNode) store.getSubNode();
        this.finalSchema = PlannerUtil.sortSpecsToSchema(sortNode.getSortKeys());
        this.sortComp = new TupleComparator(finalSchema, sortNode.getSortKeys());
      }
    }
    // for localizing the intermediate data
    localize(request);

    context.setStatus(QueryStatus.QUERY_INITED);
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
    LOG.info("* Local task dir: " + taskAttemptDir.getAbsolutePath());
    LOG.info("* plan:\n");
    LOG.info(plan.toString());
    LOG.info("==================================");
  }

  public void init() throws InternalException {
  }

  public File createLocalDir(Path path) throws IOException {
    localFS.mkdirs(path);
    Path qualified = localFS.makeQualified(path);
    return new File(qualified.toUri());
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
      Path inputDir = lDirAllocator.
          getLocalPathForWrite(
              Worker.getQueryUnitDir(context.getTaskId()).toString() + "/in", conf);

      if (!localFS.exists(inputDir)) {
        createLocalDir(inputDir);
      }

      Path qualified = localFS.makeQualified(inputDir);
      Path inFile;

      int i = fetcherRunners.size();
      for (Fragment cache : cached) {
        inFile = new Path(qualified, "in_" + i);
        workerContext.getDefaultFS().copyToLocalFile(cache.getPath(), inFile);
        cache.setPath(inFile);
        i++;
      }
    }
  }

  public QueryUnitAttemptId getId() {
    return context.getTaskId();
  }

  public QueryStatus getStatus() {
    return context.getStatus();
  }

  public String toString() {
    return "queryId: " + this.getId() + " status: " + this.getStatus();
  }

  public void setStatus(QueryStatus status) {
    context.setStatus(status);
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
    context.setStatus(QueryStatus.QUERY_KILLED);
  }

  public void abort() {
    aborted = true;
    context.stop();
    context.setStatus(QueryStatus.QUERY_ABORTED);
  }

  public void cleanUp() {
    // remove itself from worker
    // 끝난건지 확인
    if (context.getStatus() == QueryStatus.QUERY_FINISHED) {
      try {
        // context.getWorkDir() 지우기
        localFS.delete(new Path(context.getWorkDir().getAbsolutePath()), true);
        // tasks에서 자기 지우기
        synchronized (workerContext.getTasks()) {
          workerContext.getTasks().remove(this.getId());
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      LOG.error(new UnfinishedTaskException("QueryUnitAttemptId: "
          + context.getTaskId() + " status: " + context.getStatus()));
    }
  }

  public TaskStatusProto getReport() {
    TaskStatusProto.Builder builder = TaskStatusProto.newBuilder();
    builder.setId(context.getTaskId().getProto())
        .setProgress(context.getProgress())
        .setStatus(context.getStatus());

/*      if (context.getStatSet(ExprType.STORE.toString()) != null) {
        builder.setStats(context.getStatSet(ExprType.STORE.toString()).getProto());
      }*/
    if (context.hasResultStats()) {
      builder.setResultStats(context.getResultStats().getProto());
    } else {
      builder.setResultStats(new TableStat().getProto());
    }

    String dataServerURL = workerContext.getDataServerURL();

    if (context.getStatus() == QueryStatus.QUERY_FINISHED && interQuery) {
      Iterator<Entry<Integer,String>> it = context.getRepartitions();
      if (it.hasNext()) {
        do {
          Entry<Integer,String> entry = it.next();
          Partition.Builder part = Partition.newBuilder();
          part.setPartitionKey(entry.getKey());
          if (partitionType == SubQuery.PARTITION_TYPE.HASH) {
            part.setFileName(
                dataServerURL + "/?qid=" + getId().toString() + "&fn=" +
                    entry.getValue());
          } else if (partitionType == SubQuery.PARTITION_TYPE.LIST) {
            part.setFileName(dataServerURL + "/?qid=" + getId().toString() +
                "&fn=0");
          } else {
            part.setFileName(dataServerURL + "/?qid=" + getId().toString());
          }
          builder.addPartitions(part.build());
        } while (it.hasNext());
      }
    }

    return builder.build();
  }

  private void waitForFetch() throws InterruptedException, IOException {
    context.getFetchLatch().await();
    LOG.info(context.getTaskId() + " All fetches are done!");
    Collection<String> inputs = Lists.newArrayList(context.getInputTables());
    for (String inputTable: inputs) {
      File tableDir = new File(context.getFetchIn(), inputTable);
      Fragment [] frags = list(tableDir, inputTable,
          context.getTable(inputTable).getMeta());
      context.changeFragment(inputTable, frags);
    }
  }

  @Override
  public void run() {
    try {
      context.setStatus(QueryStatus.QUERY_INPROGRESS);
      LOG.info("Query status of " + context.getTaskId() + " is changed to " +
          getStatus());

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
      LOG.error(ExceptionUtils.getStackTrace(e));
      aborted = true;
    } finally {
      finished = true;
      if (killed || aborted) {
        context.setProgress(0.0f);
        QueryStatus failedStatus = null;
        if (killed) {
          failedStatus = QueryStatus.QUERY_KILLED;
        } else if (aborted) {
          failedStatus = QueryStatus.QUERY_ABORTED;
        }
        context.setStatus(failedStatus);
        LOG.info("Query status of " + context.getTaskId() + " is changed to "
            + failedStatus);
      } else { // if successful
        context.setProgress(1.0f);
        if (interQuery) { // TODO - to be completed
          if (partitionType == null || partitionType != SubQuery.PARTITION_TYPE.RANGE) {
            //PartitionRetrieverHandler partitionHandler =
            //new PartitionRetrieverHandler(context.getWorkDir().getAbsolutePath() + "/out/data");
            PartitionRetrieverHandler partitionHandler =
                new PartitionRetrieverHandler(
                    context.getWorkDir().getAbsolutePath() + "/out/data");
            workerContext.getRetriever().register(this.getId(),
                partitionHandler);
          } else {
            RangeRetrieverHandler rangeHandler = null;
            try {
              rangeHandler =
                  new RangeRetrieverHandler(new File(
                      context.getWorkDir() + "/out"), finalSchema, sortComp);
            } catch (IOException e) {
              LOG.error("ERROR: cannot initialize RangeRetrieverHandler");
            }
            workerContext.getRetriever().register(this.getId(), rangeHandler);
          }
          LOG.info("Worker starts to serve as HTTP data server for "
              + getId());
        }
        context.setStatus(QueryStatus.QUERY_FINISHED);
        LOG.info("Query status of " + context.getTaskId() + " is changed to "
            + QueryStatus.QUERY_FINISHED);
      }
    }
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

  private Fragment[] list(File file, String name, TableMeta meta)
      throws IOException {
    Configuration c = new Configuration(conf);
    c.set("fs.default.name", "file:///");
    FileSystem fs = FileSystem.get(c);
    Path tablePath = new Path(file.getAbsolutePath());

    List<Fragment> listTablets = new ArrayList<Fragment>();
    Fragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus f : fileLists) {
      tablet = new Fragment(name, f.getPath(), meta, 0l, f.getLen());
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
          getLocalPathForWrite(
              Worker.getQueryUnitDir(ctx.getTaskId()).toString() + "/in", conf);
      createLocalDir(inputDir);
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
}
