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
import org.apache.tajo.*;
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
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.*;
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
  private final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/BenchmarkSort";
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private Path testDir;

  private final int numTuple = 10000;
  private Random rnd = new Random(System.currentTimeMillis());

  private TableDesc employee;

  String[] QUERIES = {
      "select col0 from employee order by col0"
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

    Schema schema = SchemaBuilder.builder().addAll(new Column[] {
        new Column("col0", Type.INT8),
        new Column("col1", Type.INT4),
        new Column("col2", Type.INT2),
        new Column("col3", Type.DATE),
        new Column("col4", Type.TIMESTAMP),
        new Column("col5", Type.TIME),
        new Column("col6", Type.FLOAT4),
        new Column("col7", Type.FLOAT8),
        new Column("col8", Type.INT8),
        new Column("col9", Type.INT8),
        new Column("col10", Type.INT8),
        new Column("col11", Type.INT8),
        new Column("col12", Type.INT8),
        new Column("col13", Type.INT8),
    }).build();

    TableMeta employeeMeta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, conf);
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
            NullDatum.get()
        });
      } else {
        tuple.put(new Datum[]{
            DatumFactory.createInt8(rnd.nextLong()),
            DatumFactory.createInt4(rnd.nextInt()),
            DatumFactory.createInt2((short) rnd.nextInt(Short.MAX_VALUE)),
            DatumFactory.createDate(Math.abs(rnd.nextInt())),
            DatumFactory.createTimestamp(Math.abs(rnd.nextLong())),
            DatumFactory.createTime(Math.abs(rnd.nextLong())),
            DatumFactory.createFloat4(rnd.nextFloat()),
            DatumFactory.createFloat8(rnd.nextDouble()),
            DatumFactory.createInt8(rnd.nextLong()),
            DatumFactory.createInt8(rnd.nextLong()),
            DatumFactory.createInt8(rnd.nextLong()),
            DatumFactory.createInt8(rnd.nextLong()),
            DatumFactory.createInt8(rnd.nextLong()),
            DatumFactory.createInt8(rnd.nextLong())
        });
      }
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();

    employee = new TableDesc("default.employee", schema, employeeMeta, employeePath.toUri());
    catalog.createTable(employee);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(conf, catalog, TablespaceManager.getInstance());
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
    LogicalNode rootNode = optimizer.optimize(plan);

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
    LogicalNode rootNode = optimizer.optimize(plan);

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
