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
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.apache.tajo.ipc.TajoWorkerProtocol.JoinEnforce.JoinAlgorithm;
import static org.junit.Assert.*;

public class TestHashJoinExec {
  private TajoConf conf;
  private final String TEST_PATH = "target/test-data/TestHashJoinExec";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private AbstractStorageManager sm;
  private Path testDir;
  private final Session session = LocalTajoTestingUtility.createDummySession();

  private TableDesc employee;
  private TableDesc people;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.initTestDir();
    catalog = util.startCatalogCluster().getCatalog();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    conf = util.getConfiguration();
    sm = StorageManagerFactory.getStorageManager(conf, testDir);

    Schema employeeSchema = new Schema();
    employeeSchema.addColumn("managerid", Type.INT4);
    employeeSchema.addColumn("empid", Type.INT4);
    employeeSchema.addColumn("memid", Type.INT4);
    employeeSchema.addColumn("deptname", Type.TEXT);

    TableMeta employeeMeta = CatalogUtil.newTableMeta(StoreType.CSV);
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(employeeMeta, employeeSchema,
        employeePath);
    appender.init();
    Tuple tuple = new VTuple(employeeSchema.size());
    for (int i = 0; i < 10; i++) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i),
          DatumFactory.createInt4(i), DatumFactory.createInt4(10 + i),
          DatumFactory.createText("dept_" + i) });
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();
    employee = CatalogUtil.newTableDesc("default.employee", employeeSchema, employeeMeta, employeePath);
    catalog.createTable(employee);

    Schema peopleSchema = new Schema();
    peopleSchema.addColumn("empid", Type.INT4);
    peopleSchema.addColumn("fk_memid", Type.INT4);
    peopleSchema.addColumn("name", Type.TEXT);
    peopleSchema.addColumn("age", Type.INT4);
    TableMeta peopleMeta = CatalogUtil.newTableMeta(StoreType.CSV);
    Path peoplePath = new Path(testDir, "people.csv");
    appender = StorageManagerFactory.getStorageManager(conf).getAppender(peopleMeta, peopleSchema, peoplePath);
    appender.init();
    tuple = new VTuple(peopleSchema.size());
    for (int i = 1; i < 10; i += 2) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i),
          DatumFactory.createInt4(10 + i),
          DatumFactory.createText("name_" + i),
          DatumFactory.createInt4(30 + i) });
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();

    people = CatalogUtil.newTableDesc("default.people", peopleSchema, peopleMeta, peoplePath);
    catalog.createTable(people);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  String[] QUERIES = {
      "select managerId, e.empId, deptName, e.memId from employee as e inner join " +
          "people as p on e.empId = p.empId and e.memId = p.fk_memId"
  };

  @Test
  public final void testHashInnerJoin() throws IOException, PlanningException {
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalNode plan = planner.createPlan(session, expr).getRootBlock().getRoot();

    JoinNode joinNode = PlannerUtil.findTopNode(plan, NodeType.JOIN);
    Enforcer enforcer = new Enforcer();
    enforcer.enforceJoinAlgorithm(joinNode.getPID(), JoinAlgorithm.IN_MEMORY_HASH_JOIN);

    FileFragment[] empFrags = StorageManager.splitNG(conf, "default.e", employee.getMeta(), employee.getPath(), Integer.MAX_VALUE);
    FileFragment[] peopleFrags = StorageManager.splitNG(conf, "default.p", people.getMeta(), people.getPath(), Integer.MAX_VALUE);
    FileFragment[] merged = TUtil.concat(empFrags, peopleFrags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testHashInnerJoin");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(), merged, workDir);
    ctx.setEnforcer(enforcer);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;
    assertTrue(proj.getChild() instanceof HashJoinExec);

    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
    while ((tuple = exec.next()) != null) {
      count++;
      assertTrue(i == tuple.get(0).asInt4());
      assertTrue(i == tuple.get(1).asInt4());
      assertTrue(("dept_" + i).equals(tuple.get(2).asChars()));
      assertTrue(10 + i == tuple.get(3).asInt4());

      i += 2;
    }
    exec.close();
    assertEquals(10 / 2, count);
  }

  @Test
  public final void testCheckIfInMemoryInnerJoinIsPossible() throws IOException, PlanningException {
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalNode plan = planner.createPlan(session, expr).getRootBlock().getRoot();

    JoinNode joinNode = PlannerUtil.findTopNode(plan, NodeType.JOIN);
    Enforcer enforcer = new Enforcer();
    enforcer.enforceJoinAlgorithm(joinNode.getPID(), JoinAlgorithm.IN_MEMORY_HASH_JOIN);

    FileFragment[] peopleFrags = StorageManager.splitNG(conf, "default.p", people.getMeta(), people.getPath(),
        Integer.MAX_VALUE);
    FileFragment[] empFrags = StorageManager.splitNG(conf, "default.e", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    FileFragment[] merged = TUtil.concat(empFrags, peopleFrags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testHashInnerJoin");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(), merged, workDir);
    ctx.setEnforcer(enforcer);

    TajoConf localConf = new TajoConf(conf);
    localConf.setLongVar(TajoConf.ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD, 100l);
    PhysicalPlannerImpl phyPlanner = new PhysicalPlannerImpl(localConf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;
    assertTrue(proj.getChild() instanceof HashJoinExec);
    HashJoinExec joinExec = proj.getChild();

    assertCheckInnerJoinRelatedFunctions(ctx, phyPlanner, joinNode, joinExec);
  }

  /**
   * It checks inner-join related functions. It will return TRUE if left relations is smaller than right relations.
   *
   * The below unit tests will work according to which side is smaller. In this unit tests, we use two tables: p and e.
   * The table p is 75 bytes, and the table e is 140 bytes. Since we cannot expect that which side is smaller,
   * we use some boolean variable <code>leftSmaller</code> to indicate which side is small.
   */
  private static boolean assertCheckInnerJoinRelatedFunctions(TaskAttemptContext ctx,
                                                       PhysicalPlannerImpl phyPlanner,
                                                       JoinNode joinNode, BinaryPhysicalExec joinExec) throws
      IOException {

    String [] left = PlannerUtil.getRelationLineage(joinNode.getLeftChild());
    String [] right = PlannerUtil.getRelationLineage(joinNode.getRightChild());

    boolean leftSmaller;
    if (left[0].equals("default.p")) {
      leftSmaller = true;
    } else {
      leftSmaller = false;
    }

    long leftSize = phyPlanner.estimateSizeRecursive(ctx, left);
    long rightSize = phyPlanner.estimateSizeRecursive(ctx, right);

    // The table p is 75 bytes, and the table e is 140 bytes.
    if (leftSmaller) { // if left one is smaller
      assertEquals(75, leftSize);
      assertEquals(140, rightSize);
    } else { // if right one is smaller
      assertEquals(140, leftSize);
      assertEquals(75, rightSize);
    }

    if (leftSmaller) {
      PhysicalExec [] ordered = phyPlanner.switchJoinSidesIfNecessary(ctx, joinNode, joinExec.getLeftChild(),
          joinExec.getRightChild());
      assertEquals(ordered[0], joinExec.getLeftChild());
      assertEquals(ordered[1], joinExec.getRightChild());

      assertEquals("default.p", left[0]);
      assertEquals("default.e", right[0]);
    } else {
      PhysicalExec [] ordered = phyPlanner.switchJoinSidesIfNecessary(ctx, joinNode, joinExec.getLeftChild(),
          joinExec.getRightChild());
      assertEquals(ordered[1], joinExec.getLeftChild());
      assertEquals(ordered[0], joinExec.getRightChild());

      assertEquals("default.e", left[0]);
      assertEquals("default.p", right[0]);
    }

    if (leftSmaller) {
      assertTrue(phyPlanner.checkIfInMemoryInnerJoinIsPossible(ctx, joinNode.getLeftChild(), true));
      assertFalse(phyPlanner.checkIfInMemoryInnerJoinIsPossible(ctx, joinNode.getRightChild(), false));
    } else {
      assertFalse(phyPlanner.checkIfInMemoryInnerJoinIsPossible(ctx, joinNode.getLeftChild(), true));
      assertTrue(phyPlanner.checkIfInMemoryInnerJoinIsPossible(ctx, joinNode.getRightChild(), false));
    }

    return leftSmaller;
  }
}
