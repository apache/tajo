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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.engine.planner.logical.CreateTableNode;
import org.apache.tajo.engine.planner.logical.InsertNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.StoreTableNode;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.text.NumberFormat;

public abstract class ColPartitionStoreExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(ColPartitionStoreExec.class);

  protected final TableMeta meta;
  protected final StoreTableNode plan;
  protected Path storeTablePath;

  protected final int keyNum;
  protected final int [] keyIds;
  protected final String [] keyNames;

  static final ThreadLocal<NumberFormat> OUTPUT_FILE_FORMAT_SEQ =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(3);
          return fmt;
        }
      };

  public ColPartitionStoreExec(TaskAttemptContext context, StoreTableNode plan, PhysicalExec child) {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;

    if (plan.getType() == NodeType.CREATE_TABLE) {
      this.outSchema = ((CreateTableNode)plan).getTableSchema();
    }

    // set table meta
    if (this.plan.hasOptions()) {
      meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(plan.getStorageType());
    }

    // Find column index to name subpartition directory path
    keyNum = this.plan.getPartitionMethod().getExpressionSchema().size();

    keyIds = new int[keyNum];
    keyNames = new String[keyNum];
    for (int i = 0; i < keyNum; i++) {
      Column column = this.plan.getPartitionMethod().getExpressionSchema().getColumn(i);
      keyNames[i] = column.getSimpleName();

      if (this.plan.getType() == NodeType.INSERT) {
        InsertNode insertNode = ((InsertNode)plan);
        int idx = insertNode.getTableSchema().getColumnId(column.getQualifiedName());
        keyIds[i] = idx;
      } else if (this.plan.getType() == NodeType.CREATE_TABLE) {
        CreateTableNode createTable = (CreateTableNode) plan;
        int idx = createTable.getLogicalSchema().getColumnId(column.getQualifiedName());
        keyIds[i] = idx;
      } else {
        // We can get partition column from a logical schema.
        // Don't use output schema because it is rewritten.
        keyIds[i] = plan.getOutSchema().getColumnId(column.getQualifiedName());
      }
    }
  }

  @Override
  public void init() throws IOException {
    super.init();

    storeTablePath = context.getOutputPath();
    FileSystem fs = storeTablePath.getFileSystem(context.getConf());
    if (!fs.exists(storeTablePath.getParent())) {
      fs.mkdirs(storeTablePath.getParent());
    }
  }

  protected Path getDataFile(String partition) {
    return StorageUtil.concatPath(storeTablePath.getParent(), partition, storeTablePath.getName());
  }

  protected Appender makeAppender(String partition) throws IOException {
    Path dataFile = getDataFile(partition);
    FileSystem fs = dataFile.getFileSystem(context.getConf());

    if (fs.exists(dataFile.getParent())) {
      LOG.info("Path " + dataFile.getParent() + " already exists!");
    } else {
      fs.mkdirs(dataFile.getParent());
      LOG.info("Add subpartition path directory :" + dataFile.getParent());
    }

    if (fs.exists(dataFile)) {
      LOG.info("File " + dataFile + " already exists!");
      FileStatus status = fs.getFileStatus(dataFile);
      LOG.info("File size: " + status.getLen());
    }

    Appender appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta, outSchema, dataFile);
    appender.enableStats();
    appender.init();

    return appender;
  }
}
