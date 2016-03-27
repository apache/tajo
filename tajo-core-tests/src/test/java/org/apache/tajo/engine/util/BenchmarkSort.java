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

package org.apache.tajo.engine.util;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.physical.PhysicalExec;
import org.apache.tajo.engine.planner.physical.TestExternalSortExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.FileTablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Random;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;

@State(Scope.Benchmark)
public class BenchmarkSort {
  private TajoConf conf;
  private TajoTestingCluster util;
  private final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestExternalSortExec";
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private Path testDir;

  private final int numTuple = 1_000_000;
  private Random rnd = new Random(System.currentTimeMillis());

  private TableDesc employee;

  String[] QUERIES = {
      "select managerId, empId from employee order by managerId, empId"
  };

  @State(Scope.Thread)
  public static class BenchContext {
    int sortBufferSize;
  }

  @Setup
  public void setup() throws Exception {
    this.conf = new TajoConf();
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getCatalogService();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    conf.setVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR, testDir.toString());

    Schema schema = new Schema();
    schema.addColumn("managerid", Type.INT8);
    schema.addColumn("empid", Type.INT4);
    schema.addColumn("deptname", Type.TEXT);
    schema.addColumn("col1", Type.INT8);
    schema.addColumn("col2", Type.INT8);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.INT8);
    schema.addColumn("col5", Type.INT8);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.INT8);
    schema.addColumn("col8", Type.INT8);
    schema.addColumn("col9", Type.INT8);
    schema.addColumn("col10", Type.INT8);
    schema.addColumn("col11", Type.INT8);
    schema.addColumn("col12", Type.INT8);

    TableMeta employeeMeta = CatalogUtil.newTableMeta("TEXT");
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs())
        .getAppender(employeeMeta, schema, employeePath);
    appender.enableStats();
    appender.init();
    VTuple tuple = new VTuple(schema.size());
    for (int i = 0; i < numTuple; i++) {
      if (rnd.nextInt(10000) == 0) {
        tuple.put(new Datum[] {
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
        });
      } else {
        tuple.put(new Datum[]{
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt4(rnd.nextInt(100)),
            DatumFactory.createText("dept_" + i),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
        });
      }
      appender.addTuple(tuple);
    }

//    int cnt = 0;
//    while (cnt < numTuple) {
//      int n = 100_000 + rnd.nextInt(50);
//      for (int i = 0; i < 1000 && cnt < numTuple; i++, cnt++) {
//        tuple.put(new Datum[] {
//            DatumFactory.createInt8(n),
//            DatumFactory.createInt4(rnd.nextInt(100)),
//            DatumFactory.createText("dept_" + i),
//        });
//        appender.addTuple(tuple);
//      }
//    }
//    tuple.put(new Datum[] {
//        NullDatum.get(),
//        NullDatum.get(),
//        NullDatum.get(),
//    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    employee = new TableDesc("default.employee", schema, employeeMeta, employeePath.toUri());
    catalog.createTable(employee);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
  }

  @TearDown
  public void tearDown() throws IOException {
    CommonTestingUtil.cleanupTestDir(TEST_PATH);
    util.shutdownCatalogCluster();
  }

  @Benchmark
  @BenchmarkMode(Mode.All)
  public void timSort(BenchContext context) throws InterruptedException, IOException, TajoException {
    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
//    queryContext.setInt(SessionVars.EXTSORT_BUFFER_SIZE, context.sortBufferSize);
    queryContext.setInt(SessionVars.EXTSORT_BUFFER_SIZE, 200);
    queryContext.set(SessionVars.SORT_ALGORITHM.keyname(), "TIM");

    FileFragment[] frags = FileTablespace.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getUri()), Integer.MAX_VALUE);
    Path workDir = new Path(testDir, TestExternalSortExec.class.getName());
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext,
        LocalTajoTestingUtility.newTaskAttemptId(), new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), expr);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    while (exec.next() != null) {}
    exec.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.All)
  public void lsdRadixSort(BenchContext context) throws InterruptedException, IOException, TajoException {
    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    queryContext.setInt(SessionVars.EXTSORT_BUFFER_SIZE, 200);
    queryContext.set(SessionVars.SORT_ALGORITHM.keyname(), "LSD_RADIX");

    FileFragment[] frags = FileTablespace.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getUri()), Integer.MAX_VALUE);
    Path workDir = new Path(testDir, TestExternalSortExec.class.getName());
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext,
        LocalTajoTestingUtility.newTaskAttemptId(), new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), expr);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    while (exec.next() != null) {}
    exec.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.All)
  public void msdRadixSort(BenchContext context) throws InterruptedException, IOException, TajoException {
    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    queryContext.setInt(SessionVars.EXTSORT_BUFFER_SIZE, 200);
    queryContext.set(SessionVars.SORT_ALGORITHM.keyname(), "MSD_RADIX");

    FileFragment[] frags = FileTablespace.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getUri()), Integer.MAX_VALUE);
    Path workDir = new Path(testDir, TestExternalSortExec.class.getName());
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext,
        LocalTajoTestingUtility.newTaskAttemptId(), new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), expr);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    while (exec.next() != null) {}
    exec.close();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(BenchmarkSort.class.getSimpleName())
        .warmupIterations(1)
        .measurementIterations(1)
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
