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

import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.physical.ComparableVector.ComparableTuple;
import org.apache.tajo.plan.logical.StoreTableNode;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is a physical operator to store at column partitioned table.
 */
public class HashBasedColPartitionStoreExec extends ColPartitionStoreExec {

  private final ComparableTuple partKey;
  private final Map<ComparableTuple, Appender> appenderMap = new HashMap<ComparableTuple, Appender>();

  public HashBasedColPartitionStoreExec(TaskAttemptContext context, StoreTableNode plan, PhysicalExec child)
      throws IOException {
    super(context, plan, child);
    partKey = new ComparableTuple(inSchema, keyIds);
  }

  private transient final StringBuilder sb = new StringBuilder();

  private Appender getAppender(ComparableTuple partitionKey, Tuple tuple) throws IOException {
    Appender appender = appenderMap.get(partitionKey);
    if (appender != null) {
      return appender;
    }
    sb.setLength(0);
    for (int i = 0; i < keyNum; i++) {
      if (i > 0) {
        sb.append('/');
      }
      sb.append(keyNames[i]).append('=');
      Datum datum = tuple.get(keyIds[i]);
      sb.append(StringUtils.escapePathName(datum.asChars()));
    }
    appender = getNextPartitionAppender(sb.toString());

    appenderMap.put(partitionKey.copy(), appender);
    return appender;
  }

  /* (non-Javadoc)
   * @see PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    while(!context.isStopped() && (tuple = child.next()) != null) {
      partKey.set(tuple);
      // add tuple
      getAppender(partKey, tuple).addTuple(tuple);
    }

    List<TableStats> statSet = new ArrayList<TableStats>();
    for (Appender app : appenderMap.values()) {
      app.flush();
      app.close();
      statSet.add(app.getStats());
    }

    // Collect and aggregated statistics data
    TableStats aggregated = StatisticsUtil.aggregateTableStat(statSet);
    context.setResultStats(aggregated);

    return null;
  }
}