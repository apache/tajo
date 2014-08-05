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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.RangeRetrieverHandler;
import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.worker.dataserver.retriever.FileChunk;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ColumnPartitionEnforcer.ColumnPartitionAlgorithm;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType;
import static org.apache.tajo.ipc.TajoWorkerProtocol.SortEnforce.SortAlgorithm;
import static org.junit.Assert.*;

public class TestPhysicalPlanner {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static AbstractStorageManager sm;
  private static Path testDir;
  private static Session session = LocalTajoTestingUtility.createDummySession();

  private static TableDesc employee = null;
  private static TableDesc score = null;

  private static MasterPlan masterPlan;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();

    util.startCatalogCluster();
    conf = util.getConfiguration();
    testDir = CommonTestingUtil.getTestDir("target/test-data/TestPhysicalPlanner");
    sm = StorageManagerFactory.getStorageManager(conf, testDir);
    catalog = util.getMiniCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
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
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(employeeMeta, employeeSchema,
        employeePath);
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
        employeePath);
    catalog.createTable(employee);

    Path scorePath = new Path(testDir, "score");
    TableMeta scoreMeta = CatalogUtil.newTableMeta(StoreType.CSV, new KeyValueSet());
    appender = StorageManagerFactory.getStorageManager(conf).getAppender(scoreMeta, scoreSchema, scorePath);
    appender.init();
    score = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "score"), scoreSchema, scoreMeta,
        scorePath);
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

    masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), null, null);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Path queryLocalTmpDir = new Path(conf.getVar(ConfVars.WORKER_TEMPORAL_DIR));
    FileSystem fs = queryLocalTmpDir.getFileSystem(conf);
    fs.delete(queryLocalTmpDir, true);
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.employee", employee.getMeta(),
        employee.getPath(), Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testCreateScanPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode rootNode =plan.getRootBlock().getRoot();
    optimizer.optimize(plan);


    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.employee", employee.getMeta(),
        employee.getPath(), Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testCreateScanWithFilterPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[16]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode rootNode =plan.getRootBlock().getRoot();
    optimizer.optimize(plan);


    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testGroupByPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(session, context);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(
        "target/test-data/testHashGroupByPlanWithALLField");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[15]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testSortGroupByPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[]{frags[0]}, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(session, context);
    optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
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
      "create table grouped3 partition by column (dept text,  class text) as select sum(score), max(score), min(score), deptName, class from score group by deptName, class", // 2
  };

  @Test
  public final void testStorePlan() throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    ctx.setOutputPath(new Path(workDir, "grouped1"));

    Expr context = analyzer.parse(CreateTableAsStmts[0]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode rootNode = optimizer.optimize(plan);


    TableMeta outputMeta = CatalogUtil.newTableMeta(StoreType.CSV);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getFileScanner(outputMeta, rootNode.getOutSchema(),
        ctx.getOutputPath());
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
  public final void testStorePlanWithRCFile() throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testStorePlanWithRCFile");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    ctx.setOutputPath(new Path(workDir, "grouped2"));

    Expr context = analyzer.parse(CreateTableAsStmts[1]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    TableMeta outputMeta = CatalogUtil.newTableMeta(StoreType.RCFILE);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getFileScanner(outputMeta, rootNode.getOutSchema(),
        ctx.getOutputPath());
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    ctx.setOutputPath(new Path(workDir, "grouped3"));

    Expr context = analyzer.parse(CreateTableAsStmts[2]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode rootNode = optimizer.optimize(plan);
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    assertTrue(exec instanceof SortBasedColPartitionStoreExec);
  }

  @Test
  public final void testEnforceForHashBasedColumnPartitionStorePlan() throws IOException, PlanningException {

    Expr context = analyzer.parse(CreateTableAsStmts[2]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalRootNode rootNode = (LogicalRootNode) optimizer.optimize(plan);
    CreateTableNode createTableNode = rootNode.getChild();
    Enforcer enforcer = new Enforcer();
    enforcer.enforceColumnPartitionAlgorithm(createTableNode.getPID(), ColumnPartitionAlgorithm.HASH_PARTITION);

    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(enforcer);
    ctx.setOutputPath(new Path(workDir, "grouped4"));

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    assertTrue(exec instanceof HashBasedColPartitionStoreExec);
  }

  @Test
  public final void testEnforceForSortBasedColumnPartitionStorePlan() throws IOException, PlanningException {

    Expr context = analyzer.parse(CreateTableAsStmts[2]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalRootNode rootNode = (LogicalRootNode) optimizer.optimize(plan);
    CreateTableNode createTableNode = rootNode.getChild();
    Enforcer enforcer = new Enforcer();
    enforcer.enforceColumnPartitionAlgorithm(createTableNode.getPID(), ColumnPartitionAlgorithm.SORT_PARTITION);

    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(enforcer);
    ctx.setOutputPath(new Path(workDir, "grouped5"));

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    assertTrue(exec instanceof SortBasedColPartitionStoreExec);
  }

  @Test
  public final void testPartitionedStorePlan() throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    QueryUnitAttemptId id = LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan);
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        id, new FileFragment[] { frags[0] },
        CommonTestingUtil.getTestDir("target/test-data/testPartitionedStorePlan"));
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(session, context);

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
    QueryId queryId = id.getQueryUnitId().getExecutionBlockId().getQueryId();
    ExecutionBlockId ebId = id.getQueryUnitId().getExecutionBlockId();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();
    ctx.getHashShuffleAppenderManager().close(ebId);

    String executionBlockBaseDir = queryId.toString() + "/output" + "/" + ebId.getId() + "/hash-shuffle";
    Path queryLocalTmpDir = new Path(conf.getVar(ConfVars.WORKER_TEMPORAL_DIR) + "/" + executionBlockBaseDir);
    FileStatus [] list = fs.listStatus(queryLocalTmpDir);

    List<FileFragment> fragments = new ArrayList<FileFragment>();
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
  public final void testPartitionedStorePlanWithEmptyGroupingSet()
      throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    QueryUnitAttemptId id = LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan);

    Path workDir = CommonTestingUtil.getTestDir(
        "target/test-data/testPartitionedStorePlanWithEmptyGroupingSet");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        id, new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[14]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode rootNode = plan.getRootBlock().getRoot();
    int numPartitions = 1;
    DataChannel dataChannel = new DataChannel(masterPlan.newExecutionBlockId(), masterPlan.newExecutionBlockId(),
        ShuffleType.HASH_SHUFFLE, numPartitions);
    dataChannel.setShuffleKeys(new Column[]{});
    ctx.setDataChannel(dataChannel);
    optimizer.optimize(plan);

    TableMeta outputMeta = CatalogUtil.newTableMeta(dataChannel.getStoreType());

    FileSystem fs = sm.getFileSystem();
    QueryId queryId = id.getQueryUnitId().getExecutionBlockId().getQueryId();
    ExecutionBlockId ebId = id.getQueryUnitId().getExecutionBlockId();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();
    ctx.getHashShuffleAppenderManager().close(ebId);

    String executionBlockBaseDir = queryId.toString() + "/output" + "/" + ebId.getId() + "/hash-shuffle";
    Path queryLocalTmpDir = new Path(conf.getVar(ConfVars.WORKER_TEMPORAL_DIR) + "/" + executionBlockBaseDir);
    FileStatus [] list = fs.listStatus(queryLocalTmpDir);

    List<FileFragment> fragments = new ArrayList<FileFragment>();
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testAggregationFunction");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[8]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    // Set all aggregation functions to the first phase mode
    GroupbyNode groupbyNode = PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);
    for (AggregationFunctionCallEval function : groupbyNode.getAggFunctions()) {
      function.setFirstPhase();
    }

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testCountFunction");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[9]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    // Set all aggregation functions to the first phase mode
    GroupbyNode groupbyNode = PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);
    for (AggregationFunctionCallEval function : groupbyNode.getAggFunctions()) {
      function.setFirstPhase();
    }

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    Tuple tuple = exec.next();
    assertEquals(30, tuple.get(0).asInt8());
    assertNull(exec.next());
    exec.close();
  }

  @Test
  public final void testGroupByWithNullValue() throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testGroupByWithNullValue");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[11]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
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
  public final void testUnionPlan() throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.employee", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testUnionPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr  context = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode rootNode = optimizer.optimize(plan);
    LogicalRootNode root = (LogicalRootNode) rootNode;
    UnionNode union = plan.createNode(UnionNode.class);
    union.setLeftChild(root.getChild());
    union.setRightChild(root.getChild());
    root.setChild(union);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
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
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testEvalExpr");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { }, workDir);
    Expr expr = analyzer.parse(QUERIES[12]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    Tuple tuple;
    exec.init();
    tuple = exec.next();
    exec.close();
    assertEquals(true, tuple.get(0).asBool());
    assertTrue(7.0d == tuple.get(1).asFloat8());

    expr = analyzer.parse(QUERIES[13]);
    plan = planner.createPlan(session, expr);
    rootNode = optimizer.optimize(plan);

    phyPlanner = new PhysicalPlannerImpl(conf, sm);
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.employee", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testCreateIndex");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    Expr context = analyzer.parse(createIndexStmt[0]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
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
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testDuplicateEliminate");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(duplicateElimination[0]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
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
  public final void testIndexedStoreExec() throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.employee", employee.getMeta(),
        employee.getPath(), Integer.MAX_VALUE);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testIndexedStoreExec");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(SORT_QUERY[0]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode rootNode = optimizer.optimize(plan);

    SortNode sortNode = PlannerUtil.findTopNode(rootNode, NodeType.SORT);
    DataChannel channel = new DataChannel(masterPlan.newExecutionBlockId(), masterPlan.newExecutionBlockId(),
        TajoWorkerProtocol.ShuffleType.RANGE_SHUFFLE);
    channel.setShuffleKeys(PlannerUtil.sortSpecsToSchema(sortNode.getSortKeys()).toArray());
    ctx.setDataChannel(channel);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    Tuple tuple;
    exec.init();
    exec.next();
    exec.close();

    Schema keySchema = new Schema();
    keySchema.addColumn("?empId", Type.INT4);
    SortSpec[] sortSpec = new SortSpec[1];
    sortSpec[0] = new SortSpec(keySchema.getColumn(0), true, false);
    TupleComparator comp = new TupleComparator(keySchema, sortSpec);
    BSTIndex bst = new BSTIndex(conf);
    BSTIndex.BSTIndexReader reader = bst.getIndexReader(new Path(workDir, "output/index"),
        keySchema, comp);
    reader.open();
    Path outputPath = StorageUtil.concatPath(workDir, "output", "output");
    TableMeta meta = CatalogUtil.newTableMeta(channel.getStoreType(), new KeyValueSet());
    SeekableScanner scanner =
        StorageManagerFactory.getSeekableScanner(conf, meta, exec.getSchema(), outputPath);
    scanner.init();

    int cnt = 0;

    while(scanner.next() != null) {
      cnt++;
    }
    scanner.reset();

    assertEquals(100 ,cnt);

    Tuple keytuple = new VTuple(1);
    for(int i = 1 ; i < 100 ; i ++) {
      keytuple.put(0, DatumFactory.createInt4(i));
      long offsets = reader.find(keytuple);
      scanner.seek(offsets);
      tuple = scanner.next();

      assertTrue("[seek check " + (i) + " ]", ("name_" + i).equals(tuple.get(0).asChars()));
      assertTrue("[seek check " + (i) + " ]" , i == tuple.get(1).asInt4());
    }


    // The below is for testing RangeRetrieverHandler.
    RowStoreEncoder encoder = RowStoreUtil.createEncoder(keySchema);
    RangeRetrieverHandler handler = new RangeRetrieverHandler(
        new File(new Path(workDir, "output").toUri()), keySchema, comp);
    Map<String,List<String>> kvs = Maps.newHashMap();
    Tuple startTuple = new VTuple(1);
    startTuple.put(0, DatumFactory.createInt4(50));
    kvs.put("start", Lists.newArrayList(
        new String(Base64.encodeBase64(
            encoder.toBytes(startTuple), false))));
    Tuple endTuple = new VTuple(1);
    endTuple.put(0, DatumFactory.createInt4(80));
    kvs.put("end", Lists.newArrayList(
        new String(Base64.encodeBase64(
            encoder.toBytes(endTuple), false))));
    FileChunk chunk = handler.get(kvs);

    scanner.seek(chunk.startOffset());
    keytuple = scanner.next();
    assertEquals(50, keytuple.get(1).asInt4());

    long endOffset = chunk.startOffset() + chunk.length();
    while((keytuple = scanner.next()) != null && scanner.getNextOffset() <= endOffset) {
      assertTrue(keytuple.get(1).asInt4() <= 80);
    }

    scanner.close();
  }

  @Test
  public final void testSortEnforcer() throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.employee", employee.getMeta(),
        employee.getPath(), Integer.MAX_VALUE);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testSortEnforcer");
    Expr context = analyzer.parse(SORT_QUERY[0]);
    LogicalPlan plan = planner.createPlan(session, context);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    SortNode sortNode = PlannerUtil.findTopNode(rootNode, NodeType.SORT);

    Enforcer enforcer = new Enforcer();
    enforcer.enforceSortAlgorithm(sortNode.getPID(), SortAlgorithm.IN_MEMORY_SORT);
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(enforcer);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    assertTrue(exec instanceof MemSortExec);

    context = analyzer.parse(SORT_QUERY[0]);
    plan = planner.createPlan(session, context);
    optimizer.optimize(plan);
    rootNode = plan.getRootBlock().getRoot();

    sortNode = PlannerUtil.findTopNode(rootNode, NodeType.SORT);

    enforcer = new Enforcer();
    enforcer.enforceSortAlgorithm(sortNode.getPID(), SortAlgorithm.MERGE_SORT);
    ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(enforcer);

    phyPlanner = new PhysicalPlannerImpl(conf,sm);
    exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    assertTrue(exec instanceof ExternalSortExec);
  }

  @Test
  public final void testGroupByEnforcer() throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.score", score.getMeta(), score.getPath(), Integer.MAX_VALUE);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testGroupByEnforcer");
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(session, context);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    GroupbyNode groupByNode = PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);

    Enforcer enforcer = new Enforcer();
    enforcer.enforceHashAggregation(groupByNode.getPID());
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(enforcer);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    assertNotNull(PhysicalPlanUtil.findExecutor(exec, HashAggregateExec.class));

    context = analyzer.parse(QUERIES[7]);
    plan = planner.createPlan(session, context);
    optimizer.optimize(plan);
    rootNode = plan.getRootBlock().getRoot();

    groupByNode = PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);

    enforcer = new Enforcer();
    enforcer.enforceSortAggregation(groupByNode.getPID(), null);
    ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] {frags[0]}, workDir);
    ctx.setEnforcer(enforcer);

    phyPlanner = new PhysicalPlannerImpl(conf,sm);
    exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    assertTrue(exec instanceof SortAggregateExec);
  }
}