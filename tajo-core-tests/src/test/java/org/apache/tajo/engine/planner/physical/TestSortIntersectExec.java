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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.SortNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSortIntersectExec {
  private TajoConf conf;
  private final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestSortIntersectExec";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private Path testDir;

  private TableDesc employee1;
  private TableDesc employee2;

  private int[] leftNum = new int[] {1, 2, 3, 3, 9, 9, 3, 0, 3};
  private int[] rightNum = new int[] {3, 7, 3, 5};
  private int[] answerAllNum = new int[] {3, 3}; // this should be set as leftNum intersect all rightNum + order by
  private int[] answerDistinctNum = new int[] {3}; // this should be set as leftNum intersect rightNum + order by

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.initTestDir();
    catalog = util.startCatalogCluster().getCatalog();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    conf = util.getConfiguration();

    Schema employeeSchema1 = new Schema();
    employeeSchema1.addColumn("managerid", TajoDataTypes.Type.INT4);
    employeeSchema1.addColumn("empid", TajoDataTypes.Type.INT4);
    employeeSchema1.addColumn("memid", TajoDataTypes.Type.INT4);
    employeeSchema1.addColumn("deptname", TajoDataTypes.Type.TEXT);

    TableMeta employeeMeta1 = CatalogUtil.newTableMeta("TEXT");
    Path employeePath1 = new Path(testDir, "employee1.csv");
    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs()).
        getAppender(employeeMeta1, employeeSchema1, employeePath1);
    appender.init();
    Tuple tuple = new VTuple(employeeSchema1.size());

    for (int i : leftNum) {
      tuple.put(new Datum[] {
          DatumFactory.createInt4(i),
          DatumFactory.createInt4(i), // empid [0-8]
          DatumFactory.createInt4(10 + i),
          DatumFactory.createText("dept_" + i) });
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();
    employee1 = CatalogUtil.newTableDesc("default.employee1", employeeSchema1, employeeMeta1, employeePath1);
    catalog.createTable(employee1);

    Schema employeeSchema2 = new Schema();
    employeeSchema2.addColumn("managerid", TajoDataTypes.Type.INT4);
    employeeSchema2.addColumn("empid", TajoDataTypes.Type.INT4);
    employeeSchema2.addColumn("memid", TajoDataTypes.Type.INT4);
    employeeSchema2.addColumn("deptname", TajoDataTypes.Type.TEXT);

    TableMeta employeeMeta2 = CatalogUtil.newTableMeta("TEXT");
    Path employeePath2 = new Path(testDir, "employee2.csv");
    Appender appender2 = ((FileTablespace) TablespaceManager.getLocalFs()).
        getAppender(employeeMeta2, employeeSchema2, employeePath2);
    appender2.init();
    Tuple tuple2 = new VTuple(employeeSchema2.size());

    for (int i : rightNum) {
      tuple2.put(new Datum[]{
          DatumFactory.createInt4(i),
          DatumFactory.createInt4(i), // empid [1-9]
          DatumFactory.createInt4(10 + i),
          DatumFactory.createText("dept_" + i)});
      appender2.addTuple(tuple2);
    }

    appender2.flush();
    appender2.close();
    employee2 = CatalogUtil.newTableDesc("default.employee2", employeeSchema2, employeeMeta2, employeePath2);
    catalog.createTable(employee2);


    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(conf, catalog);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }


  // relation descriptions
  // employee1 (managerid, empid, memid, deptname)
  // employee2 (managerid, empid, memid, deptname)

  String[] QUERIES = {
      "select * from employee1 as e1, employee2 as e2 where e1.empId = e2.empId"
  };

  @Test
  public final void testSortIntersectAll() throws IOException, TajoException {
    FileFragment[] empFrags1 = ((FileTablespace) TablespaceManager.getLocalFs()).
        splitNG(conf, "default.e1", employee1.getMeta(),
            new Path(employee1.getUri()), Integer.MAX_VALUE);
    FileFragment[] empFrags2 = ((FileTablespace) TablespaceManager.getLocalFs())
        .splitNG(conf, "default.e2", employee2.getMeta(),
            new Path(employee2.getUri()), Integer.MAX_VALUE);

    FileFragment[] merged = TUtil.concat(empFrags1, empFrags2);

    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testSortIntersectAll");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(), merged, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), expr);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    // replace an equal join with sort intersect all .
    if (exec instanceof MergeJoinExec) {
      MergeJoinExec join = (MergeJoinExec) exec;
      exec = new SortIntersectExec(ctx, join.getLeftChild(), join.getRightChild(), false);
    } else if (exec instanceof HashJoinExec) {
      // we need to sort the results from both left and right children
      HashJoinExec join = (HashJoinExec) exec;
      SortSpec[] sortSpecsLeft = PlannerUtil.schemaToSortSpecs(join.getLeftChild().getSchema());
      SortSpec[] sortSpecsRight = PlannerUtil.schemaToSortSpecs(join.getRightChild().getSchema());

      SortNode leftSortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
      leftSortNode.setSortSpecs(sortSpecsLeft);
      leftSortNode.setInSchema(join.getLeftChild().getSchema());
      leftSortNode.setOutSchema(join.getLeftChild().getSchema());
      ExternalSortExec leftSort = new ExternalSortExec(ctx, leftSortNode, join.getLeftChild());

      SortNode rightSortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
      rightSortNode.setSortSpecs(sortSpecsRight);
      rightSortNode.setInSchema(join.getRightChild().getSchema());
      rightSortNode.setOutSchema(join.getRightChild().getSchema());
      ExternalSortExec rightSort = new ExternalSortExec(ctx, rightSortNode, join.getRightChild());

      exec = new SortIntersectExec(ctx, leftSort, rightSort, false);
    }

    Tuple tuple;
    int count = 0;
    int i = 0;
    exec.init();

    while ((tuple = exec.next()) != null) {
      count++;
      int answer = answerAllNum[i];
      assertTrue(answer == tuple.asDatum(0).asInt4());
      assertTrue(answer == tuple.asDatum(1).asInt4());
      assertTrue(10 + answer == tuple.asDatum(2).asInt4());
      assertTrue(("dept_" + answer).equals(tuple.asDatum(3).asChars()));

      i++;
    }
    exec.close();
    assertEquals(answerAllNum.length , count);
  }

  @Test
  public final void testSortIntersect() throws IOException, TajoException {
    FileFragment[] empFrags1 = ((FileTablespace) TablespaceManager.getLocalFs())
        .splitNG(conf, "default.e1", employee1.getMeta(),
            new Path(employee1.getUri()), Integer.MAX_VALUE);
    FileFragment[] empFrags2 = ((FileTablespace) TablespaceManager.getLocalFs())
        .splitNG(conf, "default.e2", employee2.getMeta(),
        new Path(employee2.getUri()), Integer.MAX_VALUE);

    FileFragment[] merged = TUtil.concat(empFrags1, empFrags2);

    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testSortIntersect");
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newTaskAttemptId(), merged, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), expr);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    // replace an equal join with sort intersect all .
    if (exec instanceof MergeJoinExec) {
      MergeJoinExec join = (MergeJoinExec) exec;
      exec = new SortIntersectExec(ctx, join.getLeftChild(), join.getRightChild(), true);
    } else if (exec instanceof HashJoinExec) {
      // we need to sort the results from both left and right children
      HashJoinExec join = (HashJoinExec) exec;
      SortSpec[] sortSpecsLeft = PlannerUtil.schemaToSortSpecs(join.getLeftChild().getSchema());
      SortSpec[] sortSpecsRight = PlannerUtil.schemaToSortSpecs(join.getRightChild().getSchema());

      SortNode leftSortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
      leftSortNode.setSortSpecs(sortSpecsLeft);
      leftSortNode.setInSchema(join.getLeftChild().getSchema());
      leftSortNode.setOutSchema(join.getLeftChild().getSchema());
      ExternalSortExec leftSort = new ExternalSortExec(ctx, leftSortNode, join.getLeftChild());

      SortNode rightSortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
      rightSortNode.setSortSpecs(sortSpecsRight);
      rightSortNode.setInSchema(join.getRightChild().getSchema());
      rightSortNode.setOutSchema(join.getRightChild().getSchema());
      ExternalSortExec rightSort = new ExternalSortExec(ctx, rightSortNode, join.getRightChild());

      exec = new SortIntersectExec(ctx, leftSort, rightSort, true);
    }

    Tuple tuple;
    int count = 0;
    int i = 0;
    exec.init();

    while ((tuple = exec.next()) != null) {
      count++;
      int answer = answerDistinctNum[i];
      assertTrue(answer == tuple.asDatum(0).asInt4());
      assertTrue(answer == tuple.asDatum(1).asInt4());
      assertTrue(10 + answer == tuple.asDatum(2).asInt4());
      assertTrue(("dept_" + answer).equals(tuple.asDatum(3).asChars()));

      i++;
    }
    exec.close();
    assertEquals(answerDistinctNum.length , count);
  }
}
