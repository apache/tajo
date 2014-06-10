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
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.query.QueryContext;
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

public class TestHashSemiJoinExec {
  private TajoConf conf;
  private final String TEST_PATH = "target/test-data/TestHashJoinExec";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private AbstractStorageManager sm;
  private Path testDir;

  private TableDesc employee;
  private TableDesc people;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.initTestDir();
    catalog = util.startCatalogCluster().getCatalog();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
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
    // make 27 tuples
    for (int i = 1; i < 10; i += 2) {
      // make three duplicated tuples for each tuples
      for (int j = 0; j < 3; j++) {
        tuple.put(new Datum[] {
            DatumFactory.createInt4(i), // empid [1, 3, 5, 7, 9]
            DatumFactory.createInt4(10 + i),
            DatumFactory.createText("name_" + i),
            DatumFactory.createInt4(30 + i) });
        appender.addTuple(tuple);
      }
    }

    appender.flush();
    appender.close();

    people = CatalogUtil.newTableDesc("default.people", peopleSchema, peopleMeta, peoplePath);
    catalog.createTable(people);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);
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
  public final void testHashSemiJoin() throws IOException, PlanningException {
    FileFragment[] empFrags = StorageManager.splitNG(conf, "default.e", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    FileFragment[] peopleFrags = StorageManager.splitNG(conf, "default.p", people.getMeta(), people.getPath(),
        Integer.MAX_VALUE);

    FileFragment[] merged = TUtil.concat(empFrags, peopleFrags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testHashSemiJoin");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(), merged, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    // replace an equal join with an hash anti join.
    if (exec instanceof MergeJoinExec) {
      MergeJoinExec join = (MergeJoinExec) exec;
      ExternalSortExec sortLeftChild = (ExternalSortExec) join.getLeftChild();
      ExternalSortExec sortRightChild = (ExternalSortExec) join.getRightChild();
      SeqScanExec scanLeftChild = (SeqScanExec) sortLeftChild.getChild();
      SeqScanExec scanRightChild = (SeqScanExec) sortRightChild.getChild();

      // 'people' should be outer table. So, the below code guarantees that people becomes the outer table.
      if (scanLeftChild.getTableName().equals("default.people")) {
        exec = new HashLeftSemiJoinExec(ctx, join.getPlan(), scanRightChild, scanLeftChild);
      } else {
        exec = new HashLeftSemiJoinExec(ctx, join.getPlan(), scanLeftChild, scanRightChild);
      }
    } else if (exec instanceof HashJoinExec) {
      HashJoinExec join = (HashJoinExec) exec;
      SeqScanExec scanLeftChild = (SeqScanExec) join.getLeftChild();

      // 'people' should be outer table. So, the below code guarantees that people becomes the outer table.
      if (scanLeftChild.getTableName().equals("default.people")) {
        exec = new HashLeftSemiJoinExec(ctx, join.getPlan(), join.getRightChild(), join.getLeftChild());
      } else {
        exec = new HashLeftSemiJoinExec(ctx, join.getPlan(), join.getLeftChild(), join.getRightChild());
      }
    }

    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
    // expect result without duplicated tuples.
    while ((tuple = exec.next()) != null) {
      count++;
      assertTrue(i == tuple.get(0).asInt4());
      assertTrue(i == tuple.get(1).asInt4());
      assertTrue(("dept_" + i).equals(tuple.get(2).asChars()));
      assertTrue(10 + i == tuple.get(3).asInt4());

      i += 2;
    }
    exec.close();
    assertEquals(5 , count); // the expected result: [1, 3, 5, 7, 9]
  }
}
