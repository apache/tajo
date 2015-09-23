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

import com.google.common.base.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.logical.InsertNode;
import org.apache.tajo.plan.logical.PersistentStoreNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.FileTablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * This is a physical executor to store a table part into a specified storage.
 */
public class StoreTableExec extends UnaryPhysicalExec {
  private static final Log LOG = LogFactory.getLog(StoreTableExec.class);

  private PersistentStoreNode plan;
  private TableMeta meta;
  private Appender appender;

  // for file punctuation
  private TableStats sumStats;                  // for aggregating all stats of written files
  private long maxPerFileSize = Long.MAX_VALUE; // default max file size is 2^63
  private int writtenFileNum = 0;               // how many file are written so far?
  private Path lastFileName;                    // latest written file name

  public StoreTableExec(TaskAttemptContext context, PersistentStoreNode plan, PhysicalExec child) throws IOException {
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

    if (context.getQueryContext().containsKey(SessionVars.MAX_OUTPUT_FILE_SIZE)) {
      maxPerFileSize = context.getQueryContext().getLong(SessionVars.MAX_OUTPUT_FILE_SIZE) * StorageUnit.MB;
    }

    openNewFile(writtenFileNum);
    sumStats = new TableStats();
  }

  public void openNewFile(int suffixId) throws IOException {
    Schema appenderSchema = (plan instanceof InsertNode) ? ((InsertNode) plan).getTableSchema() : outSchema;

    if (PlannerUtil.isFileStorageType(meta.getStoreType())) {
      String prevFile = null;

      lastFileName = context.getOutputPath();

      if (suffixId > 0) {
        prevFile = lastFileName.toString();
        lastFileName = new Path(lastFileName + "_" + suffixId);
      }

      FileTablespace space = TablespaceManager.get(lastFileName.toUri());
      appender = space.getAppender(meta, appenderSchema, lastFileName);

      if (suffixId > 0) {
        LOG.info(prevFile + " exceeds " + SessionVars.MAX_OUTPUT_FILE_SIZE.keyname() + " (" + maxPerFileSize + " MB), " +
            "The remain output will be written into " + lastFileName.toString());
      }
    } else {
      Path stagingDir = context.getQueryContext().getStagingDir();
      appender = TablespaceManager.get(stagingDir.toUri()).getAppender(
          context.getQueryContext(),
          context.getTaskId(),
          meta,
          appenderSchema,
          stagingDir);
    }

    appender.enableStats();
    appender.init();
  }

  /* (non-Javadoc)
   * @see PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    while(!context.isStopped() && (tuple = child.next()) != null) {
      appender.addTuple(tuple);

      if (maxPerFileSize > 0 && maxPerFileSize <= appender.getEstimatedOutputSize()) {
        appender.close();

        writtenFileNum++;
        StatisticsUtil.aggregateTableStat(sumStats, appender.getStats());
        openNewFile(writtenFileNum);
      }
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
      if (sumStats == null) {
        context.setResultStats(appender.getStats());
      } else {
        StatisticsUtil.aggregateTableStat(sumStats, appender.getStats());
        context.setResultStats(sumStats);
      }
      if (context.getTaskId() != null) {
        context.addShuffleFileOutput(0, context.getTaskId().toString());
      }
    }

    appender = null;
    plan = null;
  }
}
