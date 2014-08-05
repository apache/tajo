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

package org.apache.tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.planner.logical.ShuffleFileWriteNode;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.HashShuffleAppender;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <code>HashShuffleFileWriteExec</code> is a physical executor to store intermediate data into a number of
 * file outputs associated with shuffle keys. The file outputs are stored on local disks.
 */
public final class HashShuffleFileWriteExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(HashShuffleFileWriteExec.class);
  private ShuffleFileWriteNode plan;
  private final TableMeta meta;
  private Partitioner partitioner;
//  private final Path storeTablePath;
  private Map<Integer, HashShuffleAppender> appenderMap = new HashMap<Integer, HashShuffleAppender>();
  private final int numShuffleOutputs;
  private final int [] shuffleKeyIds;
  private HashShuffleAppenderManager hashShuffleAppenderManager;

  public HashShuffleFileWriteExec(TaskAttemptContext context, final AbstractStorageManager sm,
                                  final ShuffleFileWriteNode plan, final PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    Preconditions.checkArgument(plan.hasShuffleKeys());
    this.plan = plan;
    if (plan.hasOptions()) {
      this.meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      this.meta = CatalogUtil.newTableMeta(plan.getStorageType());
    }
    // about the shuffle
    this.numShuffleOutputs = this.plan.getNumOutputs();
    int i = 0;
    this.shuffleKeyIds = new int [this.plan.getShuffleKeys().length];
    for (Column key : this.plan.getShuffleKeys()) {
      shuffleKeyIds[i] = inSchema.getColumnId(key.getQualifiedName());
      i++;
    }
    this.partitioner = new HashPartitioner(shuffleKeyIds, numShuffleOutputs);
    this.hashShuffleAppenderManager = context.getHashShuffleAppenderManager();
  }

  @Override
  public void init() throws IOException {
    super.init();
  }
  
  private HashShuffleAppender getAppender(int partId) throws IOException {
    HashShuffleAppender appender = appenderMap.get(partId);
    if (appender == null) {
      appender = hashShuffleAppenderManager.getAppender(context.getConf(),
          context.getQueryId().getQueryUnitId().getExecutionBlockId(), partId, meta, outSchema);
      appenderMap.put(partId, appender);
    }
    return appender;
  }

  Map<Integer, Long> partitionStats = new HashMap<Integer, Long>();
  Map<Integer, List<Tuple>> partitionTuples = new HashMap<Integer, List<Tuple>>();

  @Override
  public Tuple next() throws IOException {
    try {
      Tuple tuple;
      int partId;
      int tupleCount = 0;
      long numRows = 0;
      while ((tuple = child.next()) != null) {
        tupleCount++;
        numRows++;

        partId = partitioner.getPartition(tuple);
        List<Tuple> partitionTupleList = partitionTuples.get(partId);
        if (partitionTupleList == null) {
          partitionTupleList = new ArrayList<Tuple>(1000);
          partitionTuples.put(partId, partitionTupleList);
        }
        try {
          partitionTupleList.add(tuple.clone());
        } catch (CloneNotSupportedException e) {
        }
        if (tupleCount >= 10000) {
          for (Map.Entry<Integer, List<Tuple>> entry : partitionTuples.entrySet()) {
            int appendPartId = entry.getKey();
            HashShuffleAppender appender = getAppender(appendPartId);
            long appendedSize = appender.addTuples(entry.getValue());
            Long previousSize = partitionStats.get(appendPartId);
            if (previousSize == null) {
              partitionStats.put(appendPartId, appendedSize);
            } else {
              partitionStats.put(appendPartId, appendedSize + previousSize);
            }
            entry.getValue().clear();
          }
          tupleCount = 0;
        }
      }

      // processing remained tuples
      for (Map.Entry<Integer, List<Tuple>> entry : partitionTuples.entrySet()) {
        int appendPartId = entry.getKey();
        HashShuffleAppender appender = getAppender(appendPartId);
        long appendedSize = appender.addTuples(entry.getValue());
        Long previousSize = partitionStats.get(appendPartId);
        if (previousSize == null) {
          partitionStats.put(appendPartId, appendedSize);
        } else {
          partitionStats.put(appendPartId, appendedSize + previousSize);
        }
        entry.getValue().clear();
      }

      // set table stats
      List<TableStats> statSet = new ArrayList<TableStats>();
      for (Integer eachPartId : partitionStats.keySet()) {
        HashShuffleAppender appender = appenderMap.get(eachPartId);
        TableStats appenderStat = appender.getStats();
        TableStats tableStats = null;
        try {
          tableStats = (TableStats) appenderStat.clone();
        } catch (CloneNotSupportedException e) {
          LOG.error(e);
        }
        tableStats.setNumBytes(partitionStats.get(eachPartId));
        tableStats.setReadBytes(partitionStats.get(eachPartId));

        if (numRows > 0) {
          context.addShuffleFileOutput(eachPartId,
              hashShuffleAppenderManager.getPartitionAppenderDataFile(
                  context.getQueryId().getQueryUnitId().getExecutionBlockId(), eachPartId).getName());
          context.addPartitionOutputVolume(eachPartId, tableStats.getNumBytes());
        }
        statSet.add(tableStats);
      }

      // Collect and aggregated statistics data
      TableStats aggregated = StatisticsUtil.aggregateTableStat(statSet);
      aggregated.setNumRows(numRows);
      context.setResultStats(aggregated);

      partitionStats.clear();
      partitionTuples.clear();

      return null;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  @Override
  public void rescan() throws IOException {
    // nothing to do   
  }

  @Override
  public void close() throws IOException{
    super.close();
    if (appenderMap != null) {
      appenderMap.clear();
      appenderMap = null;
    }

    partitioner = null;
    plan = null;

    progress = 1.0f;
  }
}