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

package org.apache.tajo.engine.utils;

import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.planner.physical.ScanExec;
import org.apache.tajo.engine.planner.physical.TupleList;
import org.apache.tajo.engine.planner.physical.TupleMap;
import org.apache.tajo.util.Deallocatable;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public interface CacheHolder<T> {

  /**
   * Get a shared data from the TableCache.
   */
  T getData();

  /**
   * Get a shared table stats from the TableCache.
   */
  TableStats getTableStats();

  /**
   * Release a cache to the memory.
   *
   */
  void release();

  /**
   * This is a cache-holder for a join table
   * It will release when execution block is finished
   */
  class BroadcastCacheHolder implements CacheHolder<TupleMap<TupleList>> {
    private TupleMap<TupleList> data;
    private Deallocatable rowBlock;
    private TableStats tableStats;

    public BroadcastCacheHolder(TupleMap<TupleList> data, TableStats tableStats, Deallocatable rowBlock){
      this.data = data;
      this.tableStats = tableStats;
      this.rowBlock = rowBlock;
    }

    @Override
    public TupleMap<TupleList> getData() {
      return data;
    }

    @Override
    public TableStats getTableStats(){
      return tableStats;
    }

    @Override
    public void release() {
      if(rowBlock != null) rowBlock.release();
    }

    public static TableCacheKey getCacheKey(TaskAttemptContext ctx, ScanExec scanExec) throws IOException {

      return new TableCacheKey(ctx.getTaskId().getTaskId().getExecutionBlockId().toString(),
          scanExec.getCanonicalName(), getUniqueKey(ctx, scanExec));
    }

    public static String getUniqueKey(TaskAttemptContext context, ScanExec scanExec) {
      QueryId queryId = context.getTaskId().getTaskId().getExecutionBlockId().getQueryId();
      int pid = scanExec.getScanNode().getPID();

      return queryId.toString() + "_" + pid;
    }
  }
}
