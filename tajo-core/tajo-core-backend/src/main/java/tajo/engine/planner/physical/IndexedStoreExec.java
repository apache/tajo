/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.planner.physical;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import tajo.TaskAttemptContext;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos;
import tajo.conf.TajoConf;
import tajo.engine.planner.PlannerUtil;
import tajo.storage.*;
import tajo.storage.index.bst.BSTIndex;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 *
 */
public class IndexedStoreExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(IndexedStoreExec.class);
  private final SortSpec[] sortSpecs;
  private int [] indexKeys = null;
  private Schema keySchema;

  private BSTIndex.BSTIndexWriter indexWriter;
  private TupleComparator comp;
  private FileAppender appender;
  private TableMeta meta;

  public IndexedStoreExec(final TaskAttemptContext context, final StorageManager sm,
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
    this.meta = TCatUtil
        .newTableMeta(this.outSchema, CatalogProtos.StoreType.CSV);
    FileSystem fs = new RawLocalFileSystem();
    fs.mkdirs(storeTablePath);
    this.appender = (FileAppender) StorageManager.getAppender(context.getConf(), meta,
        new Path(storeTablePath, "output"));
    this.indexWriter = bst.getIndexWriter(new Path(storeTablePath, "index"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    this.indexWriter.setLoadNum(100);
    this.indexWriter.open();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    long offset;


    while((tuple = child.next()) != null) {
      offset = appender.getOffset();
      appender.addTuple(tuple);
      keyTuple = new VTuple(keySchema.getColumnNum());
      RowStoreUtil.project(tuple, keyTuple, indexKeys);
      indexWriter.write(keyTuple, offset);
    }

    return null;
  }

  @Override
  public void rescan() throws IOException {
  }

  public void close() throws IOException {
    super.close();

    appender.flush();
    appender.close();
    indexWriter.flush();
    indexWriter.close();

    // Collect statistics data
    context.setResultStats(appender.getStats());
    context.addRepartition(0, context.getTaskId().toString());
  }
}
