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

package org.apache.tajo;

import org.apache.tajo.util.TajoIdUtils;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryIdFactory {
  public static final QueryId NULL_QUERY_ID = newQueryId(TajoIdUtils.MASTER_ID_FORMAT.format(0), 0);

  public static final DecimalFormat ID_FORMAT = new DecimalFormat("0000");

  public static final  DecimalFormat EB_ID_FORMAT = new DecimalFormat("000000");

  public static final  DecimalFormat QU_ID_FORMAT = new DecimalFormat("000000");

  public static final  DecimalFormat ATTEMPT_ID_FORMAT = new DecimalFormat("00");

  private static Map<String, AtomicInteger> queryNexIdMap = new HashMap<>();

  private static AtomicInteger nextId = new AtomicInteger(0);

  /*
   * <ul>
   * <li>QueryId == q_{masterId}_{seq}</li>
   * <li>masterId == tajoMasterId or YarnResourceManagerId</li>
   * <li>seq = TajoSeq or YarnSeq</li>
   * </ul>
   */
  public synchronized static QueryId newQueryId(String seedQueryId) {
    AtomicInteger queryNextId = queryNexIdMap.get(seedQueryId);
    if(queryNextId == null) {
      queryNextId = new AtomicInteger(0);
      queryNexIdMap.put(seedQueryId, queryNextId);
    }
    if(isYarnId(seedQueryId)) {
      String[] tokens = seedQueryId.split("_");
      return new QueryId(tokens[1], Integer.parseInt(tokens[2]));
    } else {
      int seq = queryNextId.incrementAndGet();
      if(seq >= 10000) {
        queryNextId.set(0);
        seq = queryNextId.incrementAndGet();
      }

      return new QueryId(seedQueryId, seq);
    }
  }

  public synchronized static QueryId newQueryId(long timestamp, int seq) {
    return new QueryId(String.valueOf(timestamp), seq);
  }

  public synchronized static QueryId newQueryId(String seedQueryId, int seq) {
    if(isYarnId(seedQueryId)) {
      String[] tokens = seedQueryId.split("_");
      return new QueryId(tokens[1], Integer.parseInt(tokens[2]));
    } else {
      return new QueryId(seedQueryId, seq);
    }
  }

  private static boolean isYarnId(String id) {
    return id.startsWith("application");
  }

  public synchronized static ExecutionBlockId newExecutionBlockId(QueryId queryId) {
    return new ExecutionBlockId(queryId, nextId.incrementAndGet());
  }

  public synchronized static ExecutionBlockId newExecutionBlockId(QueryId queryId, int id) {
    return new ExecutionBlockId(queryId, id);
  }

  public synchronized static TaskId newTaskId(ExecutionBlockId executionBlockId) {
    return new TaskId(executionBlockId, nextId.incrementAndGet());
  }

  public synchronized static TaskId newTaskId(ExecutionBlockId executionBlockId, int id) {
    return new TaskId(executionBlockId, id);
  }

  public synchronized static TaskAttemptId newTaskAttemptId(TaskId taskId, final int attemptId) {
    return new TaskAttemptId(taskId, attemptId);
  }
}
