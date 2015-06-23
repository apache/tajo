/*
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
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.logical.PersistentStoreNode;
import org.apache.tajo.plan.logical.StoreTableNode;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * This is a physical executor to store rows immediately.
 */
public class InsertRowsExec extends UnaryPhysicalExec {
  private static final Log LOG = LogFactory.getLog(InsertRowsExec.class);

  private PersistentStoreNode plan;
  private TableMeta meta;
  private Appender appender;
  private Tuple tuple;

  // for file punctuation
  private TableStats sumStats; // for aggregating all stats of written files

  public InsertRowsExec(TaskAttemptContext context, PersistentStoreNode plan, PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;
  }

  public void init() throws IOException {
    super.init();

    if (plan.hasOptions()) {
      meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(plan.getStorageType());
    }

    PhysicalPlanUtil.setNullCharIfNecessary(context.getQueryContext(), plan, meta);
    sumStats = new TableStats();

    StoreTableNode storeTableNode = (StoreTableNode) plan;
    appender = TablespaceManager.get(storeTableNode.getUri()).get().getAppenderForInsertRow(
        context.getQueryContext(),
        context.getTaskId(), meta, storeTableNode.getTableSchema(), context.getOutputPath());
    appender.enableStats();
    appender.init();
  }

  /* (non-Javadoc)
   * @see PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    while((tuple = child.next()) != null) {
      appender.addTuple(tuple);
    }
        
    return null;
  }

  @Override
  public void rescan() throws IOException {
    // nothing to do
  }

  public void close() throws IOException {
    super.close();

    if(appender != null){
      appender.flush();
      appender.close();

      // Collect statistics data
      StatisticsUtil.aggregateTableStat(sumStats, appender.getStats());
      context.setResultStats(sumStats);
    }

    appender = null;
    plan = null;
  }
}
