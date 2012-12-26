/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

/**
 * 
 */
package tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import tajo.TaskAttemptContext;
import tajo.catalog.Column;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.StatisticsUtil;
import tajo.catalog.statistics.TableStat;
import tajo.engine.planner.logical.StoreTableNode;
import tajo.storage.Appender;
import tajo.storage.StorageManager;
import tajo.storage.StorageUtil;
import tajo.storage.Tuple;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public final class PartitionedStoreExec extends UnaryPhysicalExec {
  private static final NumberFormat numFormat = NumberFormat.getInstance();

  static {
    numFormat.setGroupingUsed(false);
    numFormat.setMinimumIntegerDigits(6);
  }

  private final StorageManager sm;
  private final StoreTableNode plan;

  private final int numPartitions;
  private final int [] partitionKeys;  
  
  private final TableMeta meta;
  private final Partitioner partitioner;
  private final Path storeTablePath;
  private final Map<Integer, Appender> appenderMap
    = new HashMap<Integer, Appender>();
  
  public PartitionedStoreExec(TaskAttemptContext context, final StorageManager sm,
      final StoreTableNode plan, final PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    Preconditions.checkArgument(plan.hasPartitionKey());
    this.sm = sm;
    this.plan = plan;
    this.meta = TCatUtil.newTableMeta(this.outSchema, StoreType.CSV);
    
    // about the partitions
    this.numPartitions = this.plan.getNumPartitions();
    int i = 0;
    this.partitionKeys = new int [this.plan.getPartitionKeys().length];
    for (Column key : this.plan.getPartitionKeys()) {
      partitionKeys[i] = inSchema.getColumnId(key.getQualifiedName());
      i++;
    }
    this.partitioner = new HashPartitioner(partitionKeys, numPartitions);    
    storeTablePath = new Path(context.getWorkDir().getAbsolutePath(), "out");
    sm.initLocalTableBase(storeTablePath, meta);
  }
  
  private Appender getAppender(int partition) throws IOException {
    Appender appender = appenderMap.get(partition);
    if (appender == null) {
      Path dataFile = getDataFile(partition);
//      Log.info(">>>>>> " + dataFile.toString());
      appender = sm.getLocalAppender(meta, dataFile);      
      appenderMap.put(partition, appender);
    } else {
      appender = appenderMap.get(partition);
    }
    
    return appender;
  }

  private Path getDataFile(int partition) {
    return StorageUtil.concatPath(storeTablePath, "data", "" + partition);
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    Appender appender;
    int partition;
    while ((tuple = child.next()) != null) {
      partition = partitioner.getPartition(tuple);
      appender = getAppender(partition);
      appender.addTuple(tuple);
    }
    
    List<TableStat> statSet = new ArrayList<TableStat>();
    for (Map.Entry<Integer, Appender> entry : appenderMap.entrySet()) {
      int partNum = entry.getKey();
      Appender app = entry.getValue();
      app.flush();
      app.close();
      statSet.add(app.getStats());
      if (app.getStats().getNumRows() > 0) {
        context.addRepartition(partNum, getDataFile(partNum).getName());
      }
    }
    
    // Collect and aggregated statistics data
    TableStat aggregated = StatisticsUtil.aggregateTableStat(statSet);
    context.setResultStats(aggregated);
    
    return null;
  }

  @Override
  public void rescan() throws IOException {
    // nothing to do   
  }
}