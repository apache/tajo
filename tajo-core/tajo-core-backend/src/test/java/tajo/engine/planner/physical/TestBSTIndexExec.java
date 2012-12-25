/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.TajoTestingCluster;
import tajo.TajoTestingUtility;
import tajo.TaskAttemptContext;
import tajo.WorkerTestingUtil;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.PhysicalPlannerImpl;
import tajo.engine.planner.PlanningContext;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.ScanNode;
import tajo.storage.*;
import tajo.storage.index.bst.BSTIndex;
import tajo.util.TUtil;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestBSTIndexExec {

  private TajoConf conf;
  private Path idxPath;
  private CatalogService catalog;
  private QueryAnalyzer analyzer;
  private LogicalPlanner planner;
  private StorageManager sm;
  private Schema idxSchema;
  private TupleComparator comp;
  private BSTIndex.BSTIndexWriter writer;
  private HashMap<Integer , Integer> randomValues ;
  private int rndKey = -1;

  private Random rnd = new Random(System.currentTimeMillis());

  private TajoTestingCluster util;

  @Before
  public void setup() throws Exception {
    this.randomValues = new HashMap<Integer , Integer> ();
    this.conf = new TajoConf();
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();

    Path workDir = WorkerTestingUtil.buildTestDir("target/test-data/TestPhysicalPlanner");
    sm = StorageManager.get(conf, workDir);

    idxPath = new Path(workDir, "test.idx");

    Schema schema = new Schema();
    schema.addColumn("managerId", DataType.INT);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);

    this.idxSchema = new Schema();
    idxSchema.addColumn("managerId", DataType.INT);
    SortSpec[] sortKeys = new SortSpec[1];
    sortKeys[0] = new SortSpec(idxSchema.getColumn("managerId"), true, false);
    this.comp = new TupleComparator(idxSchema, sortKeys);

    this.writer = new BSTIndex(conf).getIndexWriter(idxPath,
        BSTIndex.TWO_LEVEL_INDEX, this.idxSchema, this.comp);
    writer.setLoadNum(100);
    writer.open();
    long offset;

    TableMeta employeeMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    sm.initTableBase(employeeMeta, "employee");
    FileAppender appender = (FileAppender) sm.getAppender(employeeMeta,
        "employee", "employee");
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < 10000; i++) {
      
      Tuple key = new VTuple(this.idxSchema.getColumnNum());
      int rndKey = rnd.nextInt(250);
      if(this.randomValues.containsKey(rndKey)) {
        int t = this.randomValues.remove(rndKey) + 1;
        this.randomValues.put(rndKey, t);
      } else {
        this.randomValues.put(rndKey, 1);
      }
      
      key.put(new Datum[] { DatumFactory.createInt(rndKey) });
      tuple.put(new Datum[] { DatumFactory.createInt(rndKey),
          DatumFactory.createInt(rnd.nextInt(10)),
          DatumFactory.createString("dept_" + rnd.nextInt(10)) });
      offset = appender.getOffset();
      appender.addTuple(tuple);
      writer.write(key, offset);
    }
    appender.flush();
    appender.close();
    writer.close();

    TableDesc desc = new TableDescImpl("employee", employeeMeta,
        sm.getTablePath("employee"));
    catalog.addTable(desc);

    analyzer = new QueryAnalyzer(catalog);
    planner = new LogicalPlanner(catalog);
  }

  @After
  public void tearDown() {
    util.shutdownCatalogCluster();
  }

  @Test
  public void testEqual() throws Exception {
    
    this.rndKey = rnd.nextInt(250);
    final String QUERY = "select * from employee where managerId = " + rndKey;
    
    Fragment[] frags = sm.split("employee");
    File workDir = TajoTestingUtility.getTestDir("TestBSTIndex");
    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        TUtil.newQueryUnitAttemptId(), new Fragment[] { frags[0] }, workDir);
    PlanningContext context = analyzer.parse(QUERY);
    LogicalNode plan = planner.createPlan(context);

    plan =  LogicalOptimizer.optimize(context, plan);

    TmpPlanner phyPlanner = new TmpPlanner(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    int tupleCount = this.randomValues.get(rndKey);
    int counter = 0;

    while (exec.next() != null) {
      counter ++;
    }
    assertEquals(tupleCount , counter);
  }

  @After
  public void shutdown() {

  }

  private class TmpPlanner extends PhysicalPlannerImpl {
    public TmpPlanner(TajoConf conf, StorageManager sm) {
      super(conf, sm);
    }

    @Override
    public PhysicalExec createScanPlan(TaskAttemptContext ctx, ScanNode scanNode)
        throws IOException {
      Preconditions.checkNotNull(ctx.getTable(scanNode.getTableId()),
          "Error: There is no table matched to %s", scanNode.getTableId());

      Fragment[] fragments = ctx.getTables(scanNode.getTableId());
      
      Datum[] datum = new Datum[]{DatumFactory.createInt(rndKey)};

      return new BSTIndexScanExec(ctx, sm, scanNode, fragments[0], idxPath,
          idxSchema, comp , datum);

    }
  }
}