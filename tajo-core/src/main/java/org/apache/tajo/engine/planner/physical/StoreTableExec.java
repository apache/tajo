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

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.logical.InsertNode;
import org.apache.tajo.engine.planner.logical.PersistentStoreNode;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * This is a physical executor to store a table part into a specified storage.
 */
public class StoreTableExec extends UnaryPhysicalExec {
  private PersistentStoreNode plan;
  private Appender appender;
  private Tuple tuple;

  public StoreTableExec(TaskAttemptContext context, PersistentStoreNode plan, PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;
  }

  public void init() throws IOException {
    super.init();

    TableMeta meta;
    if (plan.hasOptions()) {
      meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(plan.getStorageType());
    }

    if (plan instanceof InsertNode) {
      InsertNode createTableNode = (InsertNode) plan;
      appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta,
          createTableNode.getTableSchema(), context.getOutputPath());
    } else {
      String nullChar = context.getQueryContext().get(ConfVars.CSVFILE_NULL.varname, ConfVars.CSVFILE_NULL.defaultVal);
      meta.putOption(StorageConstants.CSVFILE_NULL, nullChar);
      appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta, outSchema,
          context.getOutputPath());
    }

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
      context.setResultStats(appender.getStats());
      if (context.getTaskId() != null) {
        context.addShuffleFileOutput(0, context.getTaskId().toString());
      }
    }

    appender = null;
    plan = null;
  }
}
