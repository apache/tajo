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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.planner.logical.ShuffleFileWriteNode;
import org.apache.tajo.storage.*;
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
  private final Path storeTablePath;
  private Map<Integer, Appender> appenderMap = new HashMap<Integer, Appender>();
  private final int numShuffleOutputs;
  private final int [] shuffleKeyIds;
  
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
    storeTablePath = new Path(context.getWorkDir(), "output");
  }

  @Override
  public void init() throws IOException {
    super.init();
    FileSystem fs = new RawLocalFileSystem();
    fs.mkdirs(storeTablePath);
  }
  
  private Appender getAppender(int partId) throws IOException {
    Appender appender = appenderMap.get(partId);

    if (appender == null) {
      Path dataFile = getDataFile(partId);
      FileSystem fs = dataFile.getFileSystem(context.getConf());
      if (fs.exists(dataFile)) {
        LOG.info("File " + dataFile + " already exists!");
        FileStatus status = fs.getFileStatus(dataFile);
        LOG.info("File size: " + status.getLen());
      }
      appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta, outSchema, dataFile);
      appender.enableStats();
      appender.init();
      appenderMap.put(partId, appender);
    } else {
      appender = appenderMap.get(partId);
    }

    return appender;
  }

  private Path getDataFile(int partId) {
    return StorageUtil.concatPath(storeTablePath, ""+partId);
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    Appender appender;
    int partId;
    while ((tuple = child.next()) != null) {
      partId = partitioner.getPartition(tuple);
      appender = getAppender(partId);
      appender.addTuple(tuple);
    }
    
    List<TableStats> statSet = new ArrayList<TableStats>();
    for (Map.Entry<Integer, Appender> entry : appenderMap.entrySet()) {
      int partNum = entry.getKey();
      Appender app = entry.getValue();
      app.flush();
      app.close();
      statSet.add(app.getStats());
      if (app.getStats().getNumRows() > 0) {
        context.addShuffleFileOutput(partNum, getDataFile(partNum).getName());
        context.addPartitionOutputVolume(partNum, app.getStats().getNumBytes());
      }
    }
    
    // Collect and aggregated statistics data
    TableStats aggregated = StatisticsUtil.aggregateTableStat(statSet);
    context.setResultStats(aggregated);
    
    return null;
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