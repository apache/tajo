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
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;


/**
 * Contains the information about executing subquery.
 */
public class TaskAttemptContext {
  private static final Log LOG = LogFactory.getLog(TaskAttemptContext.class);
  private final TajoConf conf;
  private final Map<String, List<FragmentProto>> fragmentMap = Maps.newHashMap();

  private TaskAttemptState state;
  private TableStats resultStats;
  private QueryUnitAttemptId queryId;
  private final Path workDir;
  private boolean needFetch = false;
  private CountDownLatch doneFetchPhaseSignal;
  private float progress = 0.0f;
  private float fetcherProgress = 0.0f;
  private AtomicBoolean progressChanged = new AtomicBoolean(false);

  /** a map of shuffled file outputs */
  private Map<Integer, String> shuffleFileOutputs;
  private File fetchIn;
  private boolean stopped = false;
  private boolean interQuery = false;
  private Path outputPath;
  private DataChannel dataChannel;
  private Enforcer enforcer;
  private QueryContext queryContext;

  /** a output volume for each partition */
  private Map<Integer, Long> partitionOutputVolume;

  public TaskAttemptContext(TajoConf conf, QueryContext queryContext, final QueryUnitAttemptId queryId,
                            final FragmentProto[] fragments,
                            final Path workDir) {
    this.conf = conf;
    this.queryContext = queryContext;
    this.queryId = queryId;

    if (fragments != null) {
      for (FragmentProto t : fragments) {
        if (fragmentMap.containsKey(t.getId())) {
          fragmentMap.get(t.getId()).add(t);
        } else {
          List<FragmentProto> frags = new ArrayList<FragmentProto>();
          frags.add(t);
          fragmentMap.put(t.getId(), frags);
        }
      }
    }

    this.workDir = workDir;
    this.shuffleFileOutputs = Maps.newHashMap();

    state = TaskAttemptState.TA_PENDING;

    this.partitionOutputVolume = Maps.newHashMap();
  }

  @VisibleForTesting
  public TaskAttemptContext(TajoConf conf, QueryContext queryContext, final QueryUnitAttemptId queryId,
                            final Fragment [] fragments,  final Path workDir) {
    this(conf, queryContext, queryId, FragmentConvertor.toFragmentProtoArray(fragments), workDir);
  }

  public TajoConf getConf() {
    return this.conf;
  }

  public String getConfig(String key) {
    return queryContext.get(key) != null ? queryContext.get(key) : conf.get(key);
  }
  
  public TaskAttemptState getState() {
    return this.state;
  }
  
  public void setState(TaskAttemptState state) {
    this.state = state;
    LOG.info("Query status of " + getTaskId() + " is changed to " + state);
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
      if (fragmentMap.containsKey(t.getTableName())) {
        fragmentMap.get(t.getTableName()).add(t.getProto());
      } else {
        List<FragmentProto> frags = new ArrayList<FragmentProto>();
        frags.add(t.getProto());
        fragmentMap.put(t.getTableName(), frags);
      }
    }
  }

  public void addFragments(String tableId, FragmentProto[] fragments) {
    if (fragments == null || fragments.length == 0) {
      return;
    }
    List<FragmentProto> tableFragments = fragmentMap.get(tableId);

    if (tableFragments == null) {
      tableFragments = new ArrayList<FragmentProto>();
    }

    for (FragmentProto eachFragment: fragments) {
      tableFragments.add(eachFragment);
    }
    fragmentMap.put(tableId, tableFragments);
  }

  public Path getWorkDir() {
    return this.workDir;
  }
  
  public QueryUnitAttemptId getTaskId() {
    return this.queryId;
  }
  
  public float getProgress() {
    return this.progress;
  }
  
  public void setProgress(float progress) {
    float previousProgress = this.progress;
    this.progress = progress;
    progressChanged.set(previousProgress != progress);
  }

  public boolean isPorgressChanged() {
    return progressChanged.get();
  }
  public void setExecutorProgress(float executorProgress) {
    float adjustProgress = executorProgress * (1 - fetcherProgress);
    setProgress(fetcherProgress + adjustProgress);
  }

  public void setFetcherProgress(float fetcherProgress) {
    this.fetcherProgress = fetcherProgress;
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
    return Objects.hashCode(queryId);
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof TaskAttemptContext) {
      TaskAttemptContext other = (TaskAttemptContext) obj;
      return queryId.equals(other.getTaskId());
    } else {
      return false;
    }
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }
}