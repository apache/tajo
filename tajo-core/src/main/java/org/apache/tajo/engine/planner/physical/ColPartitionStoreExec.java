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
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionKeyProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.logical.CreateTableNode;
import org.apache.tajo.plan.logical.InsertNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.StoreTableNode;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.FileTablespace;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public abstract class ColPartitionStoreExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(ColPartitionStoreExec.class);

  protected final TableMeta meta;
  protected final StoreTableNode plan;
  protected Path storeTablePath;

  protected final int keyNum;
  protected final int [] keyIds;
  protected final String [] keyNames;

  protected Appender appender;

  // for file punctuation
  protected TableStats aggregatedStats;                  // for aggregating all stats of written files
  protected long maxPerFileSize = Long.MAX_VALUE; // default max file size is 2^63
  protected int writtenFileNum = 0;               // how many file are written so far?
  protected Path lastFileName;                    // latest written file name

  public ColPartitionStoreExec(TaskAttemptContext context, StoreTableNode plan, PhysicalExec child) {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;

    this.outSchema = plan.getTableSchema();

    // set table meta
    if (this.plan.hasOptions()) {
      meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(plan.getStorageType(), context.getConf());
    }

    PhysicalPlanUtil.setNullCharIfNecessary(context.getQueryContext(), plan, meta);

    if (context.getQueryContext().containsKey(SessionVars.MAX_OUTPUT_FILE_SIZE)) {
      maxPerFileSize = context.getQueryContext().getLong(SessionVars.MAX_OUTPUT_FILE_SIZE) * StorageUnit.MB;
    }

    // Find column index to name subpartition directory path
    keyNum = this.plan.getPartitionMethod().getExpressionSchema().size();

    if (plan.getType() == NodeType.INSERT && keyNum > 0) {
      Column[] removedPartitionColumns = new Column[this.outSchema.size() - keyNum];
      System.arraycopy(this.outSchema.toArray(), 0, removedPartitionColumns, 0, removedPartitionColumns.length);
      this.outSchema = SchemaBuilder.builder().addAll(removedPartitionColumns).build();
    }

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

    aggregatedStats = new TableStats();
  }

  protected Path getDataFile(String partition) {
    return StorageUtil.concatPath(storeTablePath.getParent(), partition, storeTablePath.getName());
  }

  protected Appender getNextPartitionAppender(String partition) throws IOException {
    lastFileName = getDataFile(partition);
    FileSystem fs = lastFileName.getFileSystem(context.getConf());

    if (fs.exists(lastFileName.getParent())) {
      LOG.info("Path " + lastFileName.getParent() + " already exists!");
    } else {
      fs.mkdirs(lastFileName.getParent());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Add subpartition path directory :" + lastFileName.getParent());
      }
    }

    if (fs.exists(lastFileName)) {
      LOG.info("File " + lastFileName + " already exists!");
      FileStatus status = fs.getFileStatus(lastFileName);
      LOG.info("File size: " + status.getLen());
    }

    openAppender(0);

    addPartition(partition);

    return appender;
  }

  /**
   * Add partition information to TableStats for storing to CatalogStore.
   *
   * @param partition partition name
   * @throws IOException
   */
  private void addPartition(String partition) throws IOException {
    PartitionDescProto.Builder builder = PartitionDescProto.newBuilder();
    builder.setPartitionName(partition);

    String[] partitionKeyPairs = partition.split("/");

    for (String partitionKeyPair : partitionKeyPairs) {
      String[] split = partitionKeyPair.split("=");

      PartitionKeyProto.Builder keyBuilder = PartitionKeyProto.newBuilder();
      keyBuilder.setColumnName(split[0]);
      // Partition path have been escaped to avoid URISyntaxException. But partition value of partition keys table
      // need to contain unescaped value for comparing filter conditions in select statement.
      keyBuilder.setPartitionValue(StringUtils.unescapePathName(split[1]));

      builder.addPartitionKeys(keyBuilder.build());
    }

    if (this.plan.getUri() == null) {
      // In CTAS, the uri would be null. So, it get the uri from staging directory.
      int endIndex = storeTablePath.toString().indexOf(FileTablespace.TMP_STAGING_DIR_PREFIX);
      String outputPath = storeTablePath.toString().substring(0, endIndex);
      builder.setPath(outputPath +  partition);
    } else {
      builder.setPath(this.plan.getUri().toString() + "/" + partition);
    }

    context.addPartition(builder.build());
  }

  public void openAppender(int suffixId) throws IOException {
    Path actualFilePath = lastFileName;
    if (suffixId > 0) {
      actualFilePath = new Path(lastFileName + "_" + suffixId);
    }

    appender = ((FileTablespace) TablespaceManager.get(lastFileName.toUri()))
        .getAppender(meta, outSchema, actualFilePath);

    appender.enableStats();
    appender.init();
  }

  @Override
  public void rescan() throws IOException {
    // nothing to do
  }
}
