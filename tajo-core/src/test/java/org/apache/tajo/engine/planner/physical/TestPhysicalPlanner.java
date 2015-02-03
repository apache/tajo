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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.session.Session;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ColumnPartitionEnforcer.ColumnPartitionAlgorithm;
import static org.apache.tajo.ipc.TajoWorkerProtocol.SortEnforce.SortAlgorithm;
import static org.junit.Assert.*;

public class TestPhysicalPlanner {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static FileStorageManager sm;
  private static Path testDir;
  private static Session session = LocalTajoTestingUtility.createDummySession();
  private static QueryContext defaultContext;

  private static TableDesc employee = null;
  private static TableDesc score = null;
  private static TableDesc largeScore = null;

  private static MasterPlan masterPlan;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();

    util.startCatalogCluster();
    conf = util.getConfiguration();
    testDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestPhysicalPlanner");
    sm = (FileStorageManager)StorageManager.getFileStorageManager(conf);
    catalog = util.getMiniCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : FunctionLoader.findLegacyFunctions()) {
      catalog.createFunction(funcDesc);
    }

    Schema employeeSchema = new Schema();
    employeeSchema.addColumn("name", Type.TEXT);
    employeeSchema.addColumn("empid", Type.INT4);
    employeeSchema.addColumn("deptname", Type.TEXT);

    Schema scoreSchema = new Schema();
    scoreSchema.addColumn("deptname", Type.TEXT);
    scoreSchema.addColumn("class", Type.TEXT);
    scoreSchema.addColumn("score", Type.INT4);
    scoreSchema.addColumn("nullable", Type.TEXT);

    TableMeta employeeMeta = CatalogUtil.newTableMeta(StoreType.CSV);


    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = sm.getAppender(employeeMeta, employeeSchema, employeePath);
    appender.init();
    Tuple tuple = new VTuple(employeeSchema.size());
    for (int i = 0; i < 100; i++) {
      tuple.put(new Datum[] {DatumFactory.createText("name_" + i),
          DatumFactory.createInt4(i), DatumFactory.createText("dept_" + i)});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    employee = new TableDesc(
        CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), employeeSchema, employeeMeta,
        employeePath.toUri());
    catalog.createTable(employee);

    Path scorePath = new Path(testDir, "score");
    TableMeta scoreMeta = CatalogUtil.newTableMeta(StoreType.CSV, new KeyValueSet());
    appender = sm.getAppender(scoreMeta, scoreSchema, scorePath);
    appender.init();
    score = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "score"), scoreSchema, scoreMeta,
        scorePath.toUri());
    tuple = new VTuple(scoreSchema.size());
    int m = 0;
    for (int i = 1; i <= 5; i++) {
      for (int k = 3; k < 5; k++) {
        for (int j = 1; j <= 3; j++) {
          tuple.put(
              new Datum[] {
                  DatumFactory.createText("name_" + i), // name_1 ~ 5 (cad: // 5)
                  DatumFactory.createText(k + "rd"), // 3 or 4rd (cad: 2)
                  DatumFactory.createInt4(j), // 1 ~ 3
              m % 3 == 1 ? DatumFactory.createText("one") : NullDatum.get()});
          appender.addTuple(tuple);
          m++;
        }
      }
    }
    appender.flush();
    appender.close();
    catalog.createTable(score);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);

    defaultContext = LocalTajoTestingUtility.createDummyContext(conf);
    masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), null, null);

    createLargeScoreTable();
  }

  public static void createLargeScoreTable() throws IOException {
    // Preparing a large table
    Path scoreLargePath = new Path(testDir, "score_large");
    CommonTestingUtil.cleanupTestDir(scoreLargePath.toString());

    Schema scoreSchmea = score.getSchema();
    TableMeta scoreLargeMeta = CatalogUtil.newTableMeta(StoreType.RAW, new KeyValueSet());
    Appender appender =  ((FileStorageManager)StorageManager.getFileStorageManager(conf))
        .getAppender(scoreLargeMeta, scoreSchmea, scoreLargePath);
    appender.enableStats();
    appender.init();
    largeScore = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "score_large"), scoreSchmea, scoreLargeMeta,
        scoreLargePath.toUri());

    Tuple tuple = new VTuple(scoreSchmea.size());
    int m = 0;
    for (int i = 1; i <= 40000; i++) {
      for (int k = 3; k < 5; k++) { // |{3,4}| = 2
        for (int j = 1; j <= 3; j++) { // |{1,2,3}| = 3
          tuple.put(
              new Datum[] {
                  DatumFactory.createText("name_" + i), // name_1 ~ 5 (cad: // 5)
                  DatumFactory.createText(k + "rd"), // 3 or 4rd (cad: 2)
                  DatumFactory.createInt4(j), // 1 ~ 3
                  m % 3 == 1 ? DatumFactory.createText("one") : NullDatum.get()});
          appender.addTuple(tuple);
          m++;
        }
      }
    }
    appender.flush();
    appender.close();
    largeScore.setStats(appender.getStats());
    catalog.createTable(largeScore);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  private String[] QUERIES = {
      "select name, empId, deptName from employee", // 0
      "select name, empId, e.deptName, manager from employee as e, dept as dp", // 1
      "select name, empId, e.deptName, manager, score from employee as e, dept, score", // 2
      "select p.deptName, sum(score) from dept as p, score group by p.deptName having sum(score) > 30", // 3
      "select p.deptName, score from dept as p, score order by score asc", // 4
      "select name from employee where empId = 100", // 5
      "select deptName, class, score from score", // 6
      "select deptName, class, sum(score), max(score), min(score) from score group by deptName, class", // 7
      "select count(*), max(score), min(score) from score", // 8
      "select count(deptName) from score", // 9
      "select managerId, empId, deptName from employee order by managerId, empId desc", // 10
      "select deptName, nullable from score group by deptName, nullable", // 11
      "select 3 < 4 as ineq, 3.5 * 2 as score", // 12
      "select (1 > 0) and 3 > 1", // 13
      "select sum(score), max(score), min(score) from score", // 14
      "select deptname, sum(score), max(score), min(score) from score group by deptname", // 15
      "select name from employee where empid >= 0", // 16
  };

  @Test
  public final void testCreateScanPlan() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getPath()), Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testCreateScanPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);
    LogicalNode rootNode =plan.getRootBlock().getRoot();
    optimizer.optimize(plan);


    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    Tuple tuple;
    int i = 0;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertTrue(tuple.contains(0));
      assertTrue(tuple.contains(1));
      assertTrue(tuple.contains(2));
      i++;
    }
    exec.close();
    assertEquals(100, i);
  }

  @Test
  public final void testCreateScanWithFilterPlan() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getPath()), Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testCreateScanWithFilterPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[16]);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);
    LogicalNode rootNode =plan.getRootBlock().getRoot();
    optimizer.optimize(plan);


    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    Tuple tuple;
    int i = 0;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertTrue(tuple.contains(0));
      i++;
    }
    exec.close();
    assertEquals(100, i);
  }

  @Test
  public final void testGroupByPlan() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testGroupByPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    int i = 0;
    Tuple tuple;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    exec.close();
    assertEquals(10, i);
  }

  @Test
  public final void testHashGroupByPlanWithALLField() throws IOException, PlanningException {
    // TODO - currently, this query does not use hash-based group operator.
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + 
        "/testHashGroupByPlanWithALLField");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[15]);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    int i = 0;
    Tuple tuple;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertEquals(12, tuple.get(1).asInt4()); // sum
      assertEquals(3, tuple.get(2).asInt4()); // max
      assertEquals(1, tuple.get(3).asInt4()); // min
      i++;
    }
    exec.close();
    assertEquals(5, i);
  }

  @Test
  public final void testSortGroupByPlan() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testSortGroupByPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[]{frags[0]}, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan.getRootBlock().getRoot());

    /*HashAggregateExec hashAgg = (HashAggregateExec) exec;

    SeqScanExec scan = (SeqScanExec) hashAgg.getSubOp();

    Column [] grpColumns = hashAgg.getAnnotation().getGroupingColumns();
    QueryBlock.SortSpec [] specs = new QueryBlock.SortSpec[grpColumns.length];
    for (int i = 0; i < grpColumns.length; i++) {
      specs[i] = new QueryBlock.SortSpec(grpColumns[i], true, false);
    }
    SortNode annotation = new SortNode(specs);
    annotation.setInSchema(scan.getSchema());
    annotation.setOutSchema(scan.getSchema());
    SortExec sort = new SortExec(annotation, scan);
    exec = new SortAggregateExec(hashAgg.getAnnotation(), sort);*/

    int i = 0;
    Tuple tuple;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    assertEquals(10, i);

    exec.rescan();
    i = 0;
    while ((tuple = exec.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    exec.close();
    assertEquals(10, i);
  }

  private String[] CreateTableAsStmts = {
      "create table grouped1 as select deptName, class, sum(score), max(score), min(score) from score group by deptName, class", // 0
      "create table grouped2 using rcfile as select deptName, class, sum(score), max(score), min(score) from score group by deptName, class", // 1
      "create table grouped3 partition by column (dept text,  class text) as select sum(score), max(score), min(score), deptName, class from score group by deptName, class", // 2,
      "create table score_large_output as select * from score_large", // 4
      "CREATE TABLE score_part (deptname text, score int4, nullable text) PARTITION BY COLUMN (class text) " +
          "AS SELECT deptname, score, nullable, class from score_large" // 5
  };

  @Test
  public final void testStorePlan() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    ctx.setOutputPath(new Path(workDir, "grouped1"));

    Expr context = analyzer.parse(CreateTableAsStmts[0]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    TableMeta outputMeta = CatalogUtil.newTableMeta(StoreType.CSV);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    Scanner scanner =  ((FileStorageManager)StorageManager.getFileStorageManager(conf))
        .getFileScanner(outputMeta, rootNode.getOutSchema(), ctx.getOutputPath());
    scanner.init();
    Tuple tuple;
    int i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    assertEquals(10, i);
    scanner.close();

    // Examine the statistics information
    assertEquals(10, ctx.getResultStats().getNumRows().longValue());
  }

  @Test
  public final void testStorePlanWithMaxOutputFileSize() throws IOException, PlanningException,
      CloneNotSupportedException {

    TableStats stats = largeScore.getStats();
    assertTrue("Checking meaningfulness of test", stats.getNumBytes() > StorageUnit.MB);

    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score_large", largeScore.getMeta(),
        new Path(largeScore.getPath()), Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testStorePlanWithMaxOutputFileSize");

    QueryContext queryContext = new QueryContext(conf, session);
    queryContext.setInt(SessionVars.MAX_OUTPUT_FILE_SIZE, 1);

    TaskAttemptContext ctx = new TaskAttemptContext(
        queryContext,
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    ctx.setOutputPath(new Path(workDir, "maxOutput"));

    Expr context = analyzer.parse(CreateTableAsStmts[3]);

    LogicalPlan plan = planner.createPlan(queryContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    // executing StoreTableExec
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    // checking the number of punctuated files
    int expectedFileNum = (int) (stats.getNumBytes() / (float) StorageUnit.MB);
    FileSystem fs = ctx.getOutputPath().getFileSystem(conf);
    FileStatus [] statuses = fs.listStatus(ctx.getOutputPath().getParent());
    assertEquals(expectedFileNum, statuses.length);

    // checking the file contents
    long totalNum = 0;
    for (FileStatus status : fs.listStatus(ctx.getOutputPath().getParent())) {
      Scanner scanner =  ((FileStorageManager)StorageManager.getFileStorageManager(conf)).getFileScanner(
          CatalogUtil.newTableMeta(StoreType.CSV),
          rootNode.getOutSchema(),
          status.getPath());

      scanner.init();
      while ((scanner.next()) != null) {
        totalNum++;
      }
      scanner.close();
    }
    assertTrue(totalNum == ctx.getResultStats().getNumRows());
  }

  @Test
  public final void testStorePlanWithRCFile() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testStorePlanWithRCFile");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    ctx.setOutputPath(new Path(workDir, "grouped2"));

    Expr context = analyzer.parse(CreateTableAsStmts[1]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    TableMeta outputMeta = CatalogUtil.newTableMeta(StoreType.RCFILE);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    Scanner scanner = ((FileStorageManager)StorageManager.getFileStorageManager(conf)).getFileScanner(
        outputMeta, rootNode.getOutSchema(), ctx.getOutputPath());
    scanner.init();
    Tuple tuple;
    int i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    assertEquals(10, i);
    scanner.close();

    // Examine the statistics information
    assertEquals(10, ctx.getResultStats().getNumRows().longValue());
  }

  @Test
  public final void testEnforceForDefaultColumnPartitionStorePlan() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    ctx.setOutputPath(new Path(workDir, "grouped3"));

    Expr context = analyzer.parse(CreateTableAsStmts[2]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    assertTrue(exec instanceof SortBasedColPartitionStoreExec);
  }

  @Test
  public final void testEnforceForHashBasedColumnPartitionStorePlan() throws IOException, PlanningException {

    Expr context = analyzer.parse(CreateTableAsStmts[2]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalRootNode rootNode = (LogicalRootNode) optimizer.optimize(plan);
    CreateTableNode createTableNode = rootNode.getChild();
    Enforcer enforcer = new Enforcer();
    enforcer.enforceColumnPartitionAlgorithm(createTableNode.getPID(), ColumnPartitionAlgorithm.HASH_PARTITION);

    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(enforcer);
    ctx.setOutputPath(new Path(workDir, "grouped4"));

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    assertTrue(exec instanceof HashBasedColPartitionStoreExec);
  }

  @Test
  public final void testEnforceForSortBasedColumnPartitionStorePlan() throws IOException, PlanningException {

    Expr context = analyzer.parse(CreateTableAsStmts[2]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalRootNode rootNode = (LogicalRootNode) optimizer.optimize(plan);
    CreateTableNode createTableNode = rootNode.getChild();
    Enforcer enforcer = new Enforcer();
    enforcer.enforceColumnPartitionAlgorithm(createTableNode.getPID(), ColumnPartitionAlgorithm.SORT_PARTITION);

    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(enforcer);
    ctx.setOutputPath(new Path(workDir, "grouped5"));

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    assertTrue(exec instanceof SortBasedColPartitionStoreExec);
  }

  @Test
  public final void testPartitionedStorePlan() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    TaskAttemptId id = LocalTajoTestingUtility.newTaskAttemptId(masterPlan);
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf), id, new FileFragment[] { frags[0] },
        CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testPartitionedStorePlan"));
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);

    int numPartitions = 3;
    Column key1 = new Column("default.score.deptname", Type.TEXT);
    Column key2 = new Column("default.score.class", Type.TEXT);
    DataChannel dataChannel = new DataChannel(masterPlan.newExecutionBlockId(), masterPlan.newExecutionBlockId(),
        ShuffleType.HASH_SHUFFLE, numPartitions);
    dataChannel.setShuffleKeys(new Column[]{key1, key2});
    ctx.setDataChannel(dataChannel);
    LogicalNode rootNode = optimizer.optimize(plan);

    TableMeta outputMeta = CatalogUtil.newTableMeta(dataChannel.getStoreType());

    FileSystem fs = sm.getFileSystem();
    QueryId queryId = id.getTaskId().getExecutionBlockId().getQueryId();
    ExecutionBlockId ebId = id.getTaskId().getExecutionBlockId();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();
    ctx.getHashShuffleAppenderManager().close(ebId);

    String executionBlockBaseDir = queryId.toString() + "/output" + "/" + ebId.getId() + "/hash-shuffle";
    Path queryLocalTmpDir = new Path(conf.getVar(ConfVars.WORKER_TEMPORAL_DIR) + "/" + executionBlockBaseDir);
    FileStatus [] list = fs.listStatus(queryLocalTmpDir);

    List<Fragment> fragments = new ArrayList<Fragment>();
    for (FileStatus status : list) {
      assertTrue(status.isDirectory());
      FileStatus [] files = fs.listStatus(status.getPath());
      for (FileStatus eachFile: files) {
        fragments.add(new FileFragment("partition", eachFile.getPath(), 0, eachFile.getLen()));
      }
    }

    assertEquals(numPartitions, fragments.size());
    Scanner scanner = new MergeScanner(conf, rootNode.getOutSchema(), outputMeta, TUtil.newList(fragments));
    scanner.init();

    Tuple tuple;
    int i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    assertEquals(10, i);
    scanner.close();

    // Examine the statistics information
    assertEquals(10, ctx.getResultStats().getNumRows().longValue());

    fs.delete(queryLocalTmpDir, true);
  }

  @Test
  public final void testPartitionedStorePlanWithMaxFileSize() throws IOException, PlanningException {

    // Preparing working dir and input fragments
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score_large", largeScore.getMeta(),
        new Path(largeScore.getPath()), Integer.MAX_VALUE);
    TaskAttemptId id = LocalTajoTestingUtility.newTaskAttemptId(masterPlan);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testPartitionedStorePlanWithMaxFileSize");

    // Setting session variables
    QueryContext queryContext = new QueryContext(conf, session);
    queryContext.setInt(SessionVars.MAX_OUTPUT_FILE_SIZE, 1);

    // Preparing task context
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext, id, new FileFragment[] { frags[0] }, workDir);
    ctx.setOutputPath(new Path(workDir, "part-01-000000"));
    // SortBasedColumnPartitionStoreExec will be chosen by default.
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(CreateTableAsStmts[4]);
    LogicalPlan plan = planner.createPlan(queryContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    // Executing CREATE TABLE PARTITION BY
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    FileSystem fs = sm.getFileSystem();
    FileStatus [] list = fs.listStatus(workDir);
    // checking the number of partitions
    assertEquals(2, list.length);

    List<Fragment> fragments = Lists.newArrayList();
    int i = 0;
    for (FileStatus status : list) {
      assertTrue(status.isDirectory());

      long fileVolumSum = 0;
      FileStatus [] fileStatuses = fs.listStatus(status.getPath());
      for (FileStatus fileStatus : fileStatuses) {
        fileVolumSum += fileStatus.getLen();
        fragments.add(new FileFragment("partition", fileStatus.getPath(), 0, fileStatus.getLen()));
      }
      assertTrue("checking the meaningfulness of test", fileVolumSum > StorageUnit.MB && fileStatuses.length > 1);

      long expectedFileNum = (long) Math.ceil(fileVolumSum / (float)StorageUnit.MB);
      assertEquals(expectedFileNum, fileStatuses.length);
    }
    TableMeta outputMeta = CatalogUtil.newTableMeta(StoreType.CSV);
    Scanner scanner = new MergeScanner(conf, rootNode.getOutSchema(), outputMeta, TUtil.newList(fragments));
    scanner.init();

    long rowNum = 0;
    while (scanner.next() != null) {
      rowNum++;
    }

    // checking the number of all written rows
    assertTrue(largeScore.getStats().getNumRows() == rowNum);

    scanner.close();
  }

  @Test
  public final void testPartitionedStorePlanWithEmptyGroupingSet()
      throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    TaskAttemptId id = LocalTajoTestingUtility.newTaskAttemptId(masterPlan);

    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + 
        "/testPartitionedStorePlanWithEmptyGroupingSet");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        id, new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[14]);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);
    LogicalNode rootNode = plan.getRootBlock().getRoot();
    int numPartitions = 1;
    DataChannel dataChannel = new DataChannel(masterPlan.newExecutionBlockId(), masterPlan.newExecutionBlockId(),
        ShuffleType.HASH_SHUFFLE, numPartitions);
    dataChannel.setShuffleKeys(new Column[]{});
    ctx.setDataChannel(dataChannel);
    optimizer.optimize(plan);

    TableMeta outputMeta = CatalogUtil.newTableMeta(dataChannel.getStoreType());

    FileSystem fs = sm.getFileSystem();
    QueryId queryId = id.getTaskId().getExecutionBlockId().getQueryId();
    ExecutionBlockId ebId = id.getTaskId().getExecutionBlockId();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();
    ctx.getHashShuffleAppenderManager().close(ebId);

    String executionBlockBaseDir = queryId.toString() + "/output" + "/" + ebId.getId() + "/hash-shuffle";
    Path queryLocalTmpDir = new Path(conf.getVar(ConfVars.WORKER_TEMPORAL_DIR) + "/" + executionBlockBaseDir);
    FileStatus [] list = fs.listStatus(queryLocalTmpDir);

    List<Fragment> fragments = new ArrayList<Fragment>();
    for (FileStatus status : list) {
      assertTrue(status.isDirectory());
      FileStatus [] files = fs.listStatus(status.getPath());
      for (FileStatus eachFile: files) {
        fragments.add(new FileFragment("partition", eachFile.getPath(), 0, eachFile.getLen()));
      }
    }

    assertEquals(numPartitions, fragments.size());

    Scanner scanner = new MergeScanner(conf, rootNode.getOutSchema(), outputMeta, TUtil.newList(fragments));
    scanner.init();
    Tuple tuple;
    int i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(60, tuple.get(0).asInt4()); // sum
      assertEquals(3, tuple.get(1).asInt4()); // max
      assertEquals(1, tuple.get(2).asInt4()); // min
      i++;
    }
    assertEquals(1, i);
    scanner.close();

    // Examine the statistics information
    assertEquals(1, ctx.getResultStats().getNumRows().longValue());
    fs.delete(queryLocalTmpDir, true);
  }

  @Test
  public final void testAggregationFunction() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testAggregationFunction");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[8]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    // Set all aggregation functions to the first phase mode
    GroupbyNode groupbyNode = PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);
    for (AggregationFunctionCallEval function : groupbyNode.getAggFunctions()) {
      function.setFirstPhase();
    }

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    exec.init();
    Tuple tuple = exec.next();
    assertEquals(30, tuple.get(0).asInt8());
    assertEquals(3, tuple.get(1).asInt4());
    assertEquals(1, tuple.get(2).asInt4());
    assertNull(exec.next());
    exec.close();
  }

  @Test
  public final void testCountFunction() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testCountFunction");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[9]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    // Set all aggregation functions to the first phase mode
    GroupbyNode groupbyNode = PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);
    for (AggregationFunctionCallEval function : groupbyNode.getAggFunctions()) {
      function.setFirstPhase();
    }

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    Tuple tuple = exec.next();
    assertEquals(30, tuple.get(0).asInt8());
    assertNull(exec.next());
    exec.close();
  }

  @Test
  public final void testGroupByWithNullValue() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testGroupByWithNullValue");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[11]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    int count = 0;
    exec.init();
    while(exec.next() != null) {
      count++;
    }
    exec.close();
    assertEquals(10, count);
  }

  @Test
  public final void testUnionPlan() throws IOException, PlanningException, CloneNotSupportedException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getPath()), Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testUnionPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr  context = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);
    LogicalRootNode root = (LogicalRootNode) rootNode;
    UnionNode union = plan.createNode(UnionNode.class);
    union.setLeftChild((LogicalNode) root.getChild().clone());
    union.setRightChild((LogicalNode) root.getChild().clone());
    root.setChild(union);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, root);

    int count = 0;
    exec.init();
    while(exec.next() != null) {
      count++;
    }
    exec.close();
    assertEquals(200, count);
  }

  @Test
  public final void testEvalExpr() throws IOException, PlanningException {
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testEvalExpr");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] { }, workDir);
    Expr expr = analyzer.parse(QUERIES[12]);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    Tuple tuple;
    exec.init();
    tuple = exec.next();
    exec.close();
    assertEquals(true, tuple.get(0).asBool());
    assertTrue(7.0d == tuple.get(1).asFloat8());

    expr = analyzer.parse(QUERIES[13]);
    plan = planner.createPlan(defaultContext, expr);
    rootNode = optimizer.optimize(plan);

    phyPlanner = new PhysicalPlannerImpl(conf);
    exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    tuple = exec.next();
    exec.close();
    assertEquals(DatumFactory.createBool(true), tuple.get(0));
  }

  public final String [] createIndexStmt = {
      "create index idx_employee on employee using bst (name null first, empId desc)"
  };

  //@Test
  public final void testCreateIndex() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getPath()), Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testCreateIndex");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    Expr context = analyzer.parse(createIndexStmt[0]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    while (exec.next() != null) {
    }
    exec.close();

    FileStatus [] list = sm.getFileSystem().listStatus(StorageUtil.concatPath(workDir, "index"));
    assertEquals(2, list.length);
  }

  final static String [] duplicateElimination = {
      "select distinct deptname from score",
  };

  @Test
  public final void testDuplicateEliminate() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(),
        new Path(score.getPath()), Integer.MAX_VALUE);

    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testDuplicateEliminate");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(duplicateElimination[0]);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    Tuple tuple;

    int cnt = 0;
    Set<String> expected = Sets.newHashSet(
        "name_1", "name_2", "name_3", "name_4", "name_5");
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertTrue(expected.contains(tuple.get(0).asChars()));
      cnt++;
    }
    exec.close();
    assertEquals(5, cnt);
  }

  public String [] SORT_QUERY = {
      "select name, empId from employee order by empId"
  };

  @Test
  public final void testSortEnforcer() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getPath()), Integer.MAX_VALUE);

    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testSortEnforcer");
    Expr context = analyzer.parse(SORT_QUERY[0]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    SortNode sortNode = PlannerUtil.findTopNode(rootNode, NodeType.SORT);

    Enforcer enforcer = new Enforcer();
    enforcer.enforceSortAlgorithm(sortNode.getPID(), SortAlgorithm.IN_MEMORY_SORT);
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(enforcer);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    assertTrue(exec instanceof MemSortExec);

    context = analyzer.parse(SORT_QUERY[0]);
    plan = planner.createPlan(defaultContext, context);
    optimizer.optimize(plan);
    rootNode = plan.getRootBlock().getRoot();

    sortNode = PlannerUtil.findTopNode(rootNode, NodeType.SORT);

    enforcer = new Enforcer();
    enforcer.enforceSortAlgorithm(sortNode.getPID(), SortAlgorithm.MERGE_SORT);
    ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(enforcer);

    phyPlanner = new PhysicalPlannerImpl(conf);
    exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    assertTrue(exec instanceof ExternalSortExec);
  }

  @Test
  public final void testGroupByEnforcer() throws IOException, PlanningException {
    FileFragment[] frags = FileStorageManager.splitNG(conf, "default.score", score.getMeta(), new Path(score.getPath()),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testGroupByEnforcer");
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(defaultContext, context);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    GroupbyNode groupByNode = PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);

    Enforcer enforcer = new Enforcer();
    enforcer.enforceHashAggregation(groupByNode.getPID());
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(enforcer);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    assertNotNull(PhysicalPlanUtil.findExecutor(exec, HashAggregateExec.class));

    context = analyzer.parse(QUERIES[7]);
    plan = planner.createPlan(defaultContext, context);
    optimizer.optimize(plan);
    rootNode = plan.getRootBlock().getRoot();

    groupByNode = PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);

    enforcer = new Enforcer();
    enforcer.enforceSortAggregation(groupByNode.getPID(), null);
    ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(enforcer);

    phyPlanner = new PhysicalPlannerImpl(conf);
    exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    assertTrue(exec instanceof SortAggregateExec);
  }
}