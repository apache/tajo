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

/**
 *
 */
package org.apache.tajo.engine.planner.physical;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.logical.InsertNode;
import org.apache.tajo.engine.planner.logical.StoreTableNode;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.*;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * It stores a sorted data set into a number of partition files. It assumes that input tuples are sorted in an
 * ascending or descending order of partition columns.
 */
public class SortBasedColPartitionStoreExec extends ColPartitionStoreExec {
  private static Log LOG = LogFactory.getLog(SortBasedColPartitionStoreExec.class);

  private Tuple currentKey;
  private Tuple prevKey;

  private Appender appender;
  private TableStats aggregated;

  // for file rotating
  private long maxPerFileSize = Long.MAX_VALUE;
  private int writtenFileNum = 0;
  private Path lastFileName;
  private long writtenTupleSize = 0;

  public SortBasedColPartitionStoreExec(TaskAttemptContext context, StoreTableNode plan, PhysicalExec child)
      throws IOException {
    super(context, plan, child);

    if (context.getQueryContext().get(QueryContext.OUTPUT_PER_FILE_SIZE) != null) {
      maxPerFileSize = Long.valueOf(context.getQueryContext().get(QueryContext.OUTPUT_PER_FILE_SIZE));
    }
  }

  public void init() throws IOException {
    super.init();

    currentKey = new VTuple(keyNum);
    aggregated = new TableStats();
  }

  private Appender getAppenderForNewPartition(String partition) throws IOException {
    lastFileName = getDataFile(partition);
    FileSystem fs = lastFileName.getFileSystem(context.getConf());

    if (fs.exists(lastFileName.getParent())) {
      LOG.info("Path " + lastFileName.getParent() + " already exists!");
    } else {
      fs.mkdirs(lastFileName.getParent());
      LOG.info("Add subpartition path directory :" + lastFileName.getParent());
    }

    if (fs.exists(lastFileName)) {
      LOG.info("File " + lastFileName + " already exists!");
      FileStatus status = fs.getFileStatus(lastFileName);
      LOG.info("File size: " + status.getLen());
    }

    openNewFile(0);

    return appender;
  }

  public void openNewFile(int suffixId) throws IOException {
    Path actualFilePath = lastFileName;
    if (suffixId > 0) {
      actualFilePath = new Path(lastFileName + "_" + suffixId);
    }

    if (plan instanceof InsertNode) {
      InsertNode createTableNode = (InsertNode) plan;
      appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta,
          createTableNode.getTableSchema(), actualFilePath);
    } else {
      String nullChar = context.getQueryContext().get(TajoConf.ConfVars.CSVFILE_NULL.varname,
          TajoConf.ConfVars.CSVFILE_NULL.defaultVal);
      meta.putOption(StorageConstants.CSVFILE_NULL, nullChar);
      appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta, outSchema,
          actualFilePath);
    }

    appender.enableStats();
    appender.init();
  }

  private void fillKeyTuple(Tuple inTuple, Tuple keyTuple) {
    for (int i = 0; i < keyIds.length; i++) {
      keyTuple.put(i, inTuple.get(keyIds[i]));
    }
  }

  private String getSubdirectory(Tuple keyTuple) {
    StringBuilder sb = new StringBuilder();

    for(int i = 0; i < keyIds.length; i++) {
      Datum datum = keyTuple.get(i);
      if(i > 0) {
        sb.append("/");
      }
      sb.append(keyNames[i]).append("=");
      sb.append(datum.asChars());
    }
    return sb.toString();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    while((tuple = child.next()) != null) {

      fillKeyTuple(tuple, currentKey);

      if (prevKey == null) {
        appender = getAppenderForNewPartition(getSubdirectory(currentKey));
        prevKey = new VTuple(currentKey);
      } else {

        if (!prevKey.equals(currentKey)) { // new partition
          appender.close();
          StatisticsUtil.aggregateTableStat(aggregated, appender.getStats());

          appender = getAppenderForNewPartition(getSubdirectory(currentKey));
          prevKey = new VTuple(currentKey);

          // reset all states for file rotating
          writtenTupleSize = 0;
          writtenFileNum = 0;
        }
      }

      appender.addTuple(tuple);
      writtenTupleSize += MemoryUtil.calculateMemorySize(tuple);

      if (writtenTupleSize > maxPerFileSize) {
        appender.close();
        writtenFileNum++;
        StatisticsUtil.aggregateTableStat(aggregated, appender.getStats());

        openNewFile(writtenFileNum);
        writtenTupleSize = 0;
      }
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    if (appender != null) {
      appender.close();
      StatisticsUtil.aggregateTableStat(aggregated, appender.getStats());
      context.setResultStats(aggregated);
    }
  }

  @Override
  public void rescan() throws IOException {
    // nothing to do
  }
}
