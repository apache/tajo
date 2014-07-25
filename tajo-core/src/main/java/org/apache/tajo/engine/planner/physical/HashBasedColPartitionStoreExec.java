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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.logical.StoreTableNode;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.Tuple;
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
  private static Log LOG = LogFactory.getLog(HashBasedColPartitionStoreExec.class);

  private final Map<String, Appender> appenderMap = new HashMap<String, Appender>();

  public HashBasedColPartitionStoreExec(TaskAttemptContext context, StoreTableNode plan, PhysicalExec child)
      throws IOException {
    super(context, plan, child);
  }

  public void init() throws IOException {
    super.init();
  }

  private Appender getAppender(String partition) throws IOException {
    Appender appender = appenderMap.get(partition);

    if (appender == null) {
      appender = makeAppender(partition);
      appenderMap.put(partition, appender);
    } else {
      appender = appenderMap.get(partition);
    }
    return appender;
  }

  /* (non-Javadoc)
   * @see PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    StringBuilder sb = new StringBuilder();
    while((tuple = child.next()) != null) {
      // set subpartition directory name
      sb.delete(0, sb.length());
      if (keyIds != null) {
        for(int i = 0; i < keyIds.length; i++) {
          Datum datum = tuple.get(keyIds[i]);
          if(i > 0)
            sb.append("/");
          sb.append(keyNames[i]).append("=");
          sb.append(datum.asChars());
        }
      }

      // add tuple
      Appender appender = getAppender(sb.toString());
      appender.addTuple(tuple);
    }

    List<TableStats> statSet = new ArrayList<TableStats>();
    for (Map.Entry<String, Appender> entry : appenderMap.entrySet()) {
      Appender app = entry.getValue();
      app.flush();
      app.close();
      statSet.add(app.getStats());
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
}