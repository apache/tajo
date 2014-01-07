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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.logical.StoreTableNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionsType;

/**
 * This class is a physical operator to store at column partitioned table.
 */
public class ColumnPartitionedTableStoreExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(ColumnPartitionedTableStoreExec.class);

  private final TableMeta meta;
  private final StoreTableNode plan;
  private Tuple tuple;
  private Path storeTablePath;
  private final Map<String, Appender> appenderMap = new HashMap<String, Appender>();
  private int[] partitionColumnIndices;
  private String[] partitionColumnNames;

  public ColumnPartitionedTableStoreExec(TaskAttemptContext context, StoreTableNode plan, PhysicalExec child)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;

    // set table meta
    if (this.plan.hasOptions()) {
      meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(plan.getStorageType());
    }

    // Rewrite a output schema because we don't have to store field values
    // corresponding to partition key columns.
    if (plan.getPartitions() != null && plan.getPartitions().getPartitionsType() == PartitionsType.COLUMN) {
      rewriteColumnPartitionedTableSchema();
    }

    // Find column index to name subpartition directory path
    if (this.plan.getPartitions() != null) {
      if (this.plan.getPartitions().getColumns() != null) {
        partitionColumnIndices = new int[plan.getPartitions().getColumns().size()];
        partitionColumnNames = new String[partitionColumnIndices.length];
        Schema columnPartitionSchema = plan.getPartitions().getSchema();
        for(int i = 0; i < columnPartitionSchema.getColumnNum(); i++)  {
          Column targetColumn = columnPartitionSchema.getColumn(i);
          for(int j = 0; j < plan.getInSchema().getColumns().size();j++) {
            Column inputColumn = plan.getInSchema().getColumn(j);
            if (inputColumn.getColumnName().equals(targetColumn.getColumnName())) {
              partitionColumnIndices[i] = j;
              partitionColumnNames[i] = targetColumn.getColumnName();
            }
          }
        }
      }
    }
  }

  /**
   * This method rewrites an input schema of column-partitioned table because
   * there are no actual field values in data file in a column-partitioned table.
   * So, this method removes partition key columns from the input schema.
   */
  private void rewriteColumnPartitionedTableSchema() {
    PartitionDesc partitionDesc = plan.getPartitions();
    Schema columnPartitionSchema = (Schema) partitionDesc.getSchema().clone();
    String qualifier = plan.getTableName();

    outSchema = PlannerUtil.rewriteColumnPartitionedTableSchema(
                                             partitionDesc,
                                             columnPartitionSchema,
                                             outSchema,
                                             qualifier);
  }

  public void init() throws IOException {
    super.init();

    storeTablePath = context.getOutputPath();
    FileSystem fs = storeTablePath.getFileSystem(context.getConf());
    if (!fs.exists(storeTablePath.getParent())) {
      fs.mkdirs(storeTablePath.getParent());
    }
  }

  private Appender getAppender(String partition) throws IOException {
    Appender appender = appenderMap.get(partition);

    if (appender == null) {
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

      appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta, outSchema, dataFile);
      appender.enableStats();
      appender.init();
      appenderMap.put(partition, appender);
    } else {
      appender = appenderMap.get(partition);
    }
    return appender;
  }

  private Path getDataFile(String partition) {
    return StorageUtil.concatPath(storeTablePath.getParent(), partition, storeTablePath.getName());
  }

  /* (non-Javadoc)
   * @see PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    StringBuilder sb = new StringBuilder();
    while((tuple = child.next()) != null) {
      // set subpartition directory name
      sb.delete(0, sb.length());
      if (partitionColumnIndices != null) {
        for(int i = 0; i < partitionColumnIndices.length; i++) {
          Datum datum = tuple.get(partitionColumnIndices[i]);
          if(i > 0)
            sb.append("/");
          sb.append(partitionColumnNames[i]).append("=");
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
