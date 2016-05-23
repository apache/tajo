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
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
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

public class TestHashAntiJoinExec {
  private TajoConf conf;
  private final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestHashJoinExec";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private Path testDir;
  private QueryContext queryContext;

  private TableDesc employee;
  private TableDesc people;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.initTestDir();
    util.startCatalogCluster();
    catalog = util.getCatalogService();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    conf = util.getConfiguration();

    Schema employeeSchema = SchemaBuilder.builder()
        .add("managerid", Type.INT4)
        .add("empid", Type.INT4)
        .add("memid", Type.INT4)
        .add("deptname", Type.TEXT)
        .build();

    TableMeta employeeMeta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration());
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs())
        .getAppender(employeeMeta, employeeSchema, employeePath);
    appender.init();
    VTuple tuple = new VTuple(employeeSchema.size());

    for (int i = 0; i < 10; i++) {
      tuple.put(new Datum[] {
          DatumFactory.createInt4(i),
          DatumFactory.createInt4(i), // empid [0-9]
          DatumFactory.createInt4(10 + i),
          DatumFactory.createText("dept_" + i) });
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();
    employee = CatalogUtil.newTableDesc("default.employee", employeeSchema, employeeMeta, employeePath);
    catalog.createTable(employee);

    Schema peopleSchema = SchemaBuilder.builder()
        .add("empid", Type.INT4)
        .add("fk_memid", Type.INT4)
        .add("name", Type.TEXT)
        .add("age", Type.INT4)
        .build();
    TableMeta peopleMeta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration());
    Path peoplePath = new Path(testDir, "people.csv");
    appender = ((FileTablespace) TablespaceManager.getLocalFs())
        .getAppender(peopleMeta, peopleSchema, peoplePath);
    appender.init();
    tuple = new VTuple(peopleSchema.size());
    for (int i = 1; i < 10; i += 2) {
      tuple.put(new Datum[] {
          DatumFactory.createInt4(i), // empid [1, 3, 5, 7, 9]
          DatumFactory.createInt4(10 + i),
          DatumFactory.createText("name_" + i),
          DatumFactory.createInt4(30 + i) });
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();

    queryContext = new QueryContext(conf);
    people = CatalogUtil.newTableDesc("default.people", peopleSchema, peopleMeta, peoplePath);
    catalog.createTable(people);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(conf, catalog, TablespaceManager.getInstance());
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }


  // relation descriptions
  // employee (managerid, empid, memid, deptname)
  // people (empid, fk_memid, name, age)

  String[] QUERIES = {
      "select managerId, e.empId, deptName, e.memId from employee as e, people as p where e.empId = p.empId"
  };

  @Test
  public final void testHashAntiJoin() throws IOException, TajoException {
    FileFragment[] empFrags = FileTablespace.splitNG(conf, "default.e", employee.getMeta(),
        new Path(employee.getUri()), Integer.MAX_VALUE);
    FileFragment[] peopleFrags = FileTablespace.splitNG(conf, "default.p", people.getMeta(),
        new Path(people.getUri()), Integer.MAX_VALUE);

    FileFragment[] merged = TUtil.concat(empFrags, peopleFrags);

    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testHashAntiJoin");
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext,
        LocalTajoTestingUtility.newTaskAttemptId(), merged, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), expr);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    // replace an equal join with an hash anti join.
    if (exec instanceof MergeJoinExec) {
      MergeJoinExec join = (MergeJoinExec) exec;
      ExternalSortExec sortLeftChild = (ExternalSortExec) join.getLeftChild();
      ExternalSortExec sortRightChild = (ExternalSortExec) join.getRightChild();
      SeqScanExec scanLeftChild = sortLeftChild.getChild();
      SeqScanExec scanRightChild = sortRightChild.getChild();

      // 'people' should be outer table. So, the below code guarantees that people becomes the outer table.
      if (scanLeftChild.getTableName().equals("default.people")) {
        exec = new HashLeftAntiJoinExec(ctx, join.getPlan(), scanRightChild, scanLeftChild);
      } else {
        exec = new HashLeftAntiJoinExec(ctx, join.getPlan(), scanLeftChild, scanRightChild);
      }
    } else if (exec instanceof HashJoinExec) {
      HashJoinExec join = (HashJoinExec) exec;
      SeqScanExec scanLeftChild = (SeqScanExec) join.getLeftChild();

      // 'people' should be outer table. So, the below code guarantees that people becomes the outer table.
      if (scanLeftChild.getTableName().equals("default.people")) {
        exec = new HashLeftAntiJoinExec(ctx, join.getPlan(), join.getRightChild(), join.getLeftChild());
      } else {
        exec = new HashLeftAntiJoinExec(ctx, join.getPlan(), join.getLeftChild(), join.getRightChild());
      }
    }

    Tuple tuple;
    int count = 0;
    int i = 0;
    exec.init();
    while ((tuple = exec.next()) != null) {
      count++;
      assertTrue(i == tuple.getInt4(0));
      assertTrue(i == tuple.getInt4(1)); // expected empid [0, 2, 4, 6, 8]
      assertTrue(("dept_" + i).equals(tuple.getText(2)));
      assertTrue(10 + i == tuple.getInt4(3));

      i += 2;
    }
    exec.close();
    assertEquals(5 , count); // the expected result : [0, 2, 4, 6, 8]
  }
}
