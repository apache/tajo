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
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.MasterPlan;
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

public class TestNLJoinExec {
  private TajoConf conf;
  private final String TEST_PATH = "target/test-data/TestNLJoinExec";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private AbstractStorageManager sm;
  private Path testDir;

  private TableDesc employee;
  private TableDesc people;

  private MasterPlan masterPlan;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    catalog = util.startCatalogCluster().getCatalog();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    conf = util.getConfiguration();
    sm = StorageManagerFactory.getStorageManager(conf, testDir);

    Schema schema = new Schema();
    schema.addColumn("managerid", Type.INT4);
    schema.addColumn("empid", Type.INT4);
    schema.addColumn("memid", Type.INT4);
    schema.addColumn("deptname", Type.TEXT);

    TableMeta employeeMeta = CatalogUtil.newTableMeta(StoreType.CSV);
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(employeeMeta, schema, employeePath);
    appender.init();
    Tuple tuple = new VTuple(schema.size());
    for (int i = 0; i < 50; i++) {
      tuple.put(new Datum[] {
          DatumFactory.createInt4(i),
          DatumFactory.createInt4(i),
          DatumFactory.createInt4(10 + i),
          DatumFactory.createText("dept_" + i)});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
    employee = CatalogUtil.newTableDesc("default.employee", schema, employeeMeta, employeePath);
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
    for (int i = 1; i < 50; i += 2) {
      tuple.put(new Datum[] {
          DatumFactory.createInt4(i),
          DatumFactory.createInt4(10 + i),
          DatumFactory.createText("name_" + i),
          DatumFactory.createInt4(30 + i)});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
    
    people = CatalogUtil.newTableDesc("default.people", peopleSchema, peopleMeta, peoplePath);
    catalog.createTable(people);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);

    masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), null, null);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }
  
  String[] QUERIES = {
    "select managerId, e.empId, deptName, e.memId from employee as e, people p",
    "select managerId, e.empId, deptName, e.memId from employee as e inner join people as p on " +
        "e.empId = p.empId and e.memId = p.fk_memId"
  };
  
  @Test
  public final void testNLCrossJoin() throws IOException, PlanningException {
    FileFragment[] empFrags = StorageManager.splitNG(conf, "default.e", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    FileFragment[] peopleFrags = StorageManager.splitNG(conf, "default.p", people.getMeta(), people.getPath(),
        Integer.MAX_VALUE);
    
    FileFragment[] merged = TUtil.concat(empFrags, peopleFrags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testNLCrossJoin");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(), merged, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[0]);
    LogicalNode plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(),
        context).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    int i = 0;
    exec.init();
    while (exec.next() != null) {
      i++;
    }
    exec.close();
    assertEquals(50*50/2, i); // expected 10 * 5
  }

  @Test
  public final void testNLInnerJoin() throws IOException, PlanningException {
    FileFragment[] empFrags = StorageManager.splitNG(conf, "default.e", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    FileFragment[] peopleFrags = StorageManager.splitNG(conf, "default.p", people.getMeta(), people.getPath(),
        Integer.MAX_VALUE);
    
    FileFragment[] merged = TUtil.concat(empFrags, peopleFrags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testNLInnerJoin");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan), merged, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context =  analyzer.parse(QUERIES[1]);
    LogicalNode plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(),
        context).getRootBlock().getRoot();
    //LogicalOptimizer.optimize(ctx, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);
    
    Tuple tuple;
    int i = 1;
    int count = 0;
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
    assertEquals(50 / 2, count);
  }
}
