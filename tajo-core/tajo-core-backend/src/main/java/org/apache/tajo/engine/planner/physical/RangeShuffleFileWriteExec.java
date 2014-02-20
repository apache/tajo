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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * <code>RangeShuffleFileWriteExec</code> is a physical executor to store intermediate data into a number of
 * file outputs associated with shuffle key ranges. The file outputs are stored with index files on local disks.
 * <code>RangeShuffleFileWriteExec</code> is implemented with an assumption that input tuples are sorted in an
 * specified order of shuffle keys.
 */
public class RangeShuffleFileWriteExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(RangeShuffleFileWriteExec.class);
  private final SortSpec[] sortSpecs;
  private int [] indexKeys = null;
  private Schema keySchema;

  private BSTIndex.BSTIndexWriter indexWriter;
  private TupleComparator comp;
  private FileAppender appender;
  private TableMeta meta;

  public RangeShuffleFileWriteExec(final TaskAttemptContext context, final AbstractStorageManager sm,
                                   final PhysicalExec child, final Schema inSchema, final Schema outSchema,
                                   final SortSpec[] sortSpecs) throws IOException {
    super(context, inSchema, outSchema, child);
    this.sortSpecs = sortSpecs;
  }

  public void init() throws IOException {
    super.init();

    indexKeys = new int[sortSpecs.length];
    keySchema = PlannerUtil.sortSpecsToSchema(sortSpecs);

    Column col;
    for (int i = 0 ; i < sortSpecs.length; i++) {
      col = sortSpecs[i].getSortKey();
      indexKeys[i] = inSchema.getColumnId(col.getQualifiedName());
    }

    BSTIndex bst = new BSTIndex(new TajoConf());
    this.comp = new TupleComparator(keySchema, sortSpecs);
    Path storeTablePath = new Path(context.getWorkDir(), "output");
    LOG.info("Output data directory: " + storeTablePath);
    this.meta = CatalogUtil.newTableMeta(context.getDataChannel() != null ?
        context.getDataChannel().getStoreType() : CatalogProtos.StoreType.RAW);
    FileSystem fs = new RawLocalFileSystem();
    fs.mkdirs(storeTablePath);
    this.appender = (FileAppender) StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta,
        outSchema, new Path(storeTablePath, "output"));
    this.appender.enableStats();
    this.appender.init();
    this.indexWriter = bst.getIndexWriter(new Path(storeTablePath, "index"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    this.indexWriter.setLoadNum(100);
    this.indexWriter.open();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    Tuple prevKeyTuple = null;
    long offset;


    while((tuple = child.next()) != null) {
      offset = appender.getOffset();
      appender.addTuple(tuple);
      keyTuple = new VTuple(keySchema.size());
      RowStoreUtil.project(tuple, keyTuple, indexKeys);
      if (prevKeyTuple == null || !prevKeyTuple.equals(keyTuple)) {
        indexWriter.write(keyTuple, offset);
        prevKeyTuple = keyTuple;
      }
    }

    return null;
  }

  @Override
  public void rescan() throws IOException {
  }

  public void close() throws IOException {
    super.close();

    appender.flush();
    IOUtils.cleanup(LOG, appender);
    indexWriter.flush();
    IOUtils.cleanup(LOG, indexWriter);

    // Collect statistics data
    context.setResultStats(appender.getStats());
    context.addShuffleFileOutput(0, context.getTaskId().toString());
    appender = null;
    indexWriter = null;
  }
}
