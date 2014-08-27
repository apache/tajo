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

import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.logical.StoreTableNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * It stores a sorted data set into a number of partition files. It assumes that input tuples are sorted in an
 * ascending or descending order of partition columns.
 */
public class SortBasedColPartitionStoreExec extends ColPartitionStoreExec {
  private Tuple currentKey;
  private Tuple prevKey;

  public SortBasedColPartitionStoreExec(TaskAttemptContext context, StoreTableNode plan, PhysicalExec child)
      throws IOException {
    super(context, plan, child);
  }

  public void init() throws IOException {
    super.init();

    currentKey = new VTuple(keyNum);
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
      sb.append(StringUtils.escapePathName(datum.asChars()));
    }
    return sb.toString();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    while((tuple = child.next()) != null) {

      fillKeyTuple(tuple, currentKey);

      if (prevKey == null) {
        appender = getNextPartitionAppender(getSubdirectory(currentKey));
        prevKey = new VTuple(currentKey);
      } else {
        if (!prevKey.equals(currentKey)) {
          appender.close();
          StatisticsUtil.aggregateTableStat(aggregatedStats, appender.getStats());

          appender = getNextPartitionAppender(getSubdirectory(currentKey));
          prevKey = new VTuple(currentKey);

          // reset all states for file rotating
          writtenFileNum = 0;
        }
      }

      appender.addTuple(tuple);

      if (maxPerFileSize > 0 && maxPerFileSize <= appender.getEstimatedOutputSize()) {
        appender.close();
        writtenFileNum++;
        StatisticsUtil.aggregateTableStat(aggregatedStats, appender.getStats());

        openAppender(writtenFileNum);
      }
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    if (appender != null) {
      appender.close();

      // Collect statistics data
      StatisticsUtil.aggregateTableStat(aggregatedStats, appender.getStats());
      context.setResultStats(aggregatedStats);
    }
  }

  @Override
  public void rescan() throws IOException {
    // nothing to do
  }
}
