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
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.plan.expr.EvalContext;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.worker.TajoWorker.WorkerContext;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;


/**
 * Contains the information about executing task attempt.
 */
public class TaskAttemptContext {
  private static final Log LOG = LogFactory.getLog(TaskAttemptContext.class);
  private final Map<String, List<FragmentProto>> fragmentMap = Maps.newHashMap();

  private volatile TaskAttemptState state;
  private TableStats resultStats;
  private TaskAttemptId taskId;
  private final Path workDir;
  private boolean needFetch = false;
  private CountDownLatch doneFetchPhaseSignal;
  private float progress = 0.0f;
  private float fetcherProgress = 0.0f;
  private volatile boolean progressChanged;

  /** a map of shuffled file outputs */
  private Map<Integer, String> shuffleFileOutputs;
  private File fetchIn;
  private volatile boolean stopped = false;
  private boolean interQuery = false;
  private Path outputPath;
  private DataChannel dataChannel;
  private Enforcer enforcer;
  private QueryContext queryContext;
  private WorkerContext workerContext;
  private ExecutionBlockSharedResource sharedResource;

  /** a output volume for each partition */
  private Map<Integer, Long> partitionOutputVolume;
  private HashShuffleAppenderManager hashShuffleAppenderManager;

  private EvalContext evalContext = new EvalContext();

  private List<PartitionDescProto> partitions;

  public TaskAttemptContext(QueryContext queryContext, final ExecutionBlockContext executionBlockContext,
                            final TaskAttemptId taskId,
                            final FragmentProto[] fragments,
                            final Path workDir) {
    this.queryContext = queryContext;

    if (executionBlockContext != null) { // For unit tests
      this.workerContext = executionBlockContext.getWorkerContext();
      this.sharedResource = executionBlockContext.getSharedResource();
    }

    this.taskId = taskId;

    if (fragments != null) {
      for (FragmentProto t : fragments) {
        if (fragmentMap.containsKey(t.getId())) {
          fragmentMap.get(t.getId()).add(t);
        } else {
          List<FragmentProto> frags = new ArrayList<>();
          frags.add(t);
          fragmentMap.put(t.getId(), frags);
        }
      }
    }

    this.workDir = workDir;
    this.shuffleFileOutputs = Maps.newHashMap();

    this.state = TaskAttemptState.TA_PENDING;

    this.partitionOutputVolume = Maps.newHashMap();

    this.partitions = new ArrayList<>();

    // set to system timezone for forwarded query
    this.evalContext.setTimeZone(queryContext.getConf().getSystemTimezone());
  }

  @VisibleForTesting
  public TaskAttemptContext(final QueryContext queryContext, final TaskAttemptId taskAttemptId,
                            final Fragment [] fragments,  final Path workDir) {
    this(queryContext, null, taskAttemptId, FragmentConvertor.toFragmentProtoArray(queryContext.getConf(), fragments),
        workDir);
  }

  public TajoConf getConf() {
    return queryContext.getConf();
  }

  public TaskAttemptState getState() {
    return this.state;
  }
  
  public void setState(TaskAttemptState state) {
    this.state = state;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Query status of " + getTaskId() + " is changed to " + state);
    }
  }

  public void setDataChannel(DataChannel dataChannel) {
    this.dataChannel = dataChannel;
  }

  public DataChannel getDataChannel() {
    return dataChannel;
  }

  public void setEnforcer(Enforcer enforcer) {
    this.enforcer = enforcer;
  }

  public Enforcer getEnforcer() {
    return this.enforcer;
  }

  public ExecutionBlockSharedResource getSharedResource() {
    return sharedResource;
  }

  public EvalNode compileEval(Schema schema, EvalNode eval) {
    return sharedResource.compileEval(schema, eval);
  }

  public EvalNode getPrecompiledEval(Schema schema, EvalNode eval) {
    if (sharedResource != null) {
      return sharedResource.getPreCompiledEval(schema, eval);
    } else {
      LOG.debug("Shared resource is not initialized. It is NORMAL in unit tests");
      return eval;
    }
  }

  public boolean hasResultStats() {
    return resultStats != null;
  }

  public void setResultStats(TableStats stats) {
    this.resultStats = stats;
  }

  public TableStats getResultStats() {
    return this.resultStats;
  }

  public boolean isStopped() {
    return this.stopped;
  }

  public void setInterQuery() {
    this.interQuery = true;
  }

  public void setOutputPath(Path outputPath) {
    this.outputPath = outputPath;
  }

  public Path getOutputPath() {
    return this.outputPath;
  }

  public boolean isInterQuery() {
    return this.interQuery;
  }
  
  public void stop() {
    this.stopped = true;
  }
  
  public void addFetchPhase(int count, File fetchIn) {
    this.needFetch = true;
    this.doneFetchPhaseSignal = new CountDownLatch(count);
    this.fetchIn = fetchIn;
  }
  
  public File getFetchIn() {
    return this.fetchIn;
  }
  
  public boolean hasFetchPhase() {
    return this.needFetch;
  }
  
  public CountDownLatch getFetchLatch() {
    return doneFetchPhaseSignal;
  }
  
  public void addShuffleFileOutput(int partId, String fileName) {
    shuffleFileOutputs.put(partId, fileName);
  }
  
  public Iterator<Entry<Integer,String>> getShuffleFileOutputs() {
    return shuffleFileOutputs.entrySet().iterator();
  }
  
  public void addPartitionOutputVolume(int partId, long volume) {
    if (partitionOutputVolume.containsKey(partId)) {
      long sum = partitionOutputVolume.get(partId);
      partitionOutputVolume.put(partId, sum + volume);
    } else {
      partitionOutputVolume.put(partId, volume);
    }
  }

  public Map<Integer, Long> getPartitionOutputVolume() {
    return partitionOutputVolume;
  }

  public void updateAssignedFragments(String tableId, Fragment[] fragments) {
    fragmentMap.remove(tableId);
    for(Fragment t : fragments) {
      if (fragmentMap.containsKey(t.getInputSourceId())) {
        fragmentMap.get(t.getInputSourceId()).add(FragmentConvertor.toFragmentProto(getConf(), t));
      } else {
        List<FragmentProto> frags = new ArrayList<>();
        frags.add(FragmentConvertor.toFragmentProto(getConf(), t));
        fragmentMap.put(t.getInputSourceId(), frags);
      }
    }
  }

  public void addFragments(String tableId, FragmentProto[] fragments) {
    if (fragments == null || fragments.length == 0) {
      return;
    }
    List<FragmentProto> tableFragments = fragmentMap.get(tableId);

    if (tableFragments == null) {
      tableFragments = new ArrayList<>();
    }

    List<Path> paths = fragmentToPath(tableFragments);

    for (FragmentProto eachFragment: fragments) {
      FileFragment fileFragment = FragmentConvertor.convert(getConf(), eachFragment);
      // If current attempt already has same path, we don't need to add it to fragments.
      if (!paths.contains(fileFragment.getPath())) {
        tableFragments.add(eachFragment);
      }
    }

    if (tableFragments.size() > 0) {
      fragmentMap.put(tableId, tableFragments);
    }
  }

  private List<Path> fragmentToPath(List<FragmentProto> tableFragments) {
    List<Path> list = new ArrayList<>();

    for (FragmentProto proto : tableFragments) {
      FileFragment fragment = FragmentConvertor.convert(getConf(), proto);
      list.add(fragment.getPath());
    }

    return list;
  }

  public Path getWorkDir() {
    return this.workDir;
  }

  //TODO change to getTaskAttemptId()
  public TaskAttemptId getTaskId() {
    return this.taskId;
  }
  
  public float getProgress() {
    return this.progress;
  }

  public void setProgress(float progress) {
    float previousProgress = this.progress;

    if (Float.isNaN(progress) || Float.isInfinite(progress)) {
      this.progress = 0.0f;
    } else {
      this.progress = progress;
    }

    this.progressChanged = previousProgress != progress;
  }

  public boolean isProgressChanged() {
    return progressChanged;
  }

  public void setExecutorProgress(float executorProgress) {
    if(Float.isNaN(executorProgress) || Float.isInfinite(executorProgress)){
      executorProgress = 0.0f;
    }

    if (hasFetchPhase()) {
      setProgress(fetcherProgress + (executorProgress * 0.5f));
    } else {
      setProgress(executorProgress);
    }
  }

  public void setFetcherProgress(float fetcherProgress) {
    if(Float.isNaN(fetcherProgress) || Float.isInfinite(fetcherProgress)){
      fetcherProgress = 0.0f;
    }
    float previousProgress = this.fetcherProgress;
    this.fetcherProgress = fetcherProgress;
    this.progressChanged = previousProgress != fetcherProgress;
  }

  public FragmentProto getTable(String id) {
    if (fragmentMap.get(id) == null) {
      //for empty table
      return null;
    }
    return fragmentMap.get(id).get(0);
  }

  public int getFragmentSize() {
    return fragmentMap.size();
  }

  public Collection<String> getInputTables() {
    return fragmentMap.keySet();
  }
  
  public FragmentProto [] getTables(String id) {
    if (fragmentMap.get(id) == null) {
      //for empty table
      return null;
    }
    return fragmentMap.get(id).toArray(new FragmentProto[fragmentMap.get(id).size()]);
  }

  public int hashCode() {
    return Objects.hashCode(taskId);
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof TaskAttemptContext) {
      TaskAttemptContext other = (TaskAttemptContext) obj;
      return taskId.equals(other.getTaskId());
    } else {
      return false;
    }
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  public HashShuffleAppenderManager getHashShuffleAppenderManager() {
    if(hashShuffleAppenderManager == null) {
      if (workerContext != null) {
        this.hashShuffleAppenderManager = workerContext.getHashShuffleAppenderManager();
      } else {
        try {
          this.hashShuffleAppenderManager = new HashShuffleAppenderManager(queryContext.getConf());
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
    return hashShuffleAppenderManager;
  }

  public EvalContext getEvalContext() {
    return evalContext;
  }

  public List<PartitionDescProto> getPartitions() {
    return partitions;
  }

  public void setPartitions(List<PartitionDescProto> partitions) {
    this.partitions = partitions;
  }

  public void addPartition(PartitionDescProto partition) {
    if (!partitions.contains(partition)) {
      partitions.add(partition);
    }
  }
}