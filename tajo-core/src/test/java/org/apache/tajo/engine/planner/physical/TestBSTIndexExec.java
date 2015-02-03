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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Stack;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;

public class TestBSTIndexExec {

  private TajoConf conf;
  private Path idxPath;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private FileStorageManager sm;
  private Schema idxSchema;
  private BaseTupleComparator comp;
  private BSTIndex.BSTIndexWriter writer;
  private HashMap<Integer , Integer> randomValues ;
  private int rndKey = -1;
  private FileSystem fs;
  private TableMeta meta;
  private Path tablePath;

  private Random rnd = new Random(System.currentTimeMillis());

  private TajoTestingCluster util;

  @Before
  public void setup() throws Exception {
    this.randomValues = new HashMap<Integer, Integer>();
    this.conf = new TajoConf();
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();

    Path workDir = CommonTestingUtil.getTestDir();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, workDir.toUri().toString());
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    sm = (FileStorageManager)StorageManager.getFileStorageManager(conf);

    idxPath = new Path(workDir, "test.idx");

    Schema schema = new Schema();
    schema.addColumn("managerid", Type.INT4);
    schema.addColumn("empid", Type.INT4);
    schema.addColumn("deptname", Type.TEXT);

    this.idxSchema = new Schema();
    idxSchema.addColumn("managerid", Type.INT4);
    SortSpec[] sortKeys = new SortSpec[1];
    sortKeys[0] = new SortSpec(idxSchema.getColumn("managerid"), true, false);
    this.comp = new BaseTupleComparator(idxSchema, sortKeys);

    this.writer = new BSTIndex(conf).getIndexWriter(idxPath,
        BSTIndex.TWO_LEVEL_INDEX, this.idxSchema, this.comp);
    writer.setLoadNum(100);
    writer.open();
    long offset;

    meta = CatalogUtil.newTableMeta(StoreType.CSV);
    tablePath = StorageUtil.concatPath(workDir, "employee", "table.csv");
    fs = tablePath.getFileSystem(conf);
    fs.mkdirs(tablePath.getParent());

    FileAppender appender = (FileAppender)sm.getAppender(meta, schema, tablePath);
    appender.init();
    Tuple tuple = new VTuple(schema.size());
    for (int i = 0; i < 10000; i++) {
      
      Tuple key = new VTuple(this.idxSchema.size());
      int rndKey = rnd.nextInt(250);
      if(this.randomValues.containsKey(rndKey)) {
        int t = this.randomValues.remove(rndKey) + 1;
        this.randomValues.put(rndKey, t);
      } else {
        this.randomValues.put(rndKey, 1);
      }
      
      key.put(new Datum[] { DatumFactory.createInt4(rndKey) });
      tuple.put(new Datum[] { DatumFactory.createInt4(rndKey),
          DatumFactory.createInt4(rnd.nextInt(10)),
          DatumFactory.createText("dept_" + rnd.nextInt(10)) });
      offset = appender.getOffset();
      appender.addTuple(tuple);
      writer.write(key, offset);
    }
    appender.flush();
    appender.close();
    writer.close();

    TableDesc desc = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, meta,
        sm.getTablePath("employee").toUri());
    catalog.createTable(desc);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);
  }

  @After
  public void tearDown() {
    util.shutdownCatalogCluster();
  }

  @Test
  public void testEqual() throws Exception {
    this.rndKey = rnd.nextInt(250);
    final String QUERY = "select * from employee where managerId = " + rndKey;
    
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.employee", meta, tablePath, Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testEqual");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(), new FileFragment[] { frags[0] }, workDir);
    Expr expr = analyzer.parse(QUERY);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    TmpPlanner phyPlanner = new TmpPlanner(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    int tupleCount = this.randomValues.get(rndKey);
    int counter = 0;
    exec.init();
    while (exec.next() != null) {
      counter ++;
    }
    exec.close();
    assertEquals(tupleCount , counter);
  }

  private class TmpPlanner extends PhysicalPlannerImpl {
    public TmpPlanner(TajoConf conf) {
      super(conf);
    }

    @Override
    public PhysicalExec createScanPlan(TaskAttemptContext ctx, ScanNode scanNode, Stack<LogicalNode> stack)
        throws IOException {
      Preconditions.checkNotNull(ctx.getTable(scanNode.getTableName()),
          "Error: There is no table matched to %s", scanNode.getTableName());

      List<FileFragment> fragments = FragmentConvertor.convert(ctx.getConf(), ctx.getTables(scanNode.getTableName()));
      
      Datum[] datum = new Datum[]{DatumFactory.createInt4(rndKey)};

      return new BSTIndexScanExec(ctx, scanNode, fragments.get(0), idxPath, idxSchema, comp , datum);

    }
  }
}