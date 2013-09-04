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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TaskAttemptContext;
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
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.SortNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.TUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMergeJoinExec {
  private TajoConf conf;
  private final String TEST_PATH = "target/test-data/TestMergeJoinExec";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private StorageManager sm;

  private TableDesc employee;
  private TableDesc people;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.initTestDir();
    catalog = util.startCatalogCluster().getCatalog();
    Path testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    conf = util.getConfiguration();
    FileSystem fs = testDir.getFileSystem(conf);
    sm = StorageManager.get(conf, testDir);

    Schema employeeSchema = new Schema();
    employeeSchema.addColumn("managerId", Type.INT4);
    employeeSchema.addColumn("empId", Type.INT4);
    employeeSchema.addColumn("memId", Type.INT4);
    employeeSchema.addColumn("deptName", Type.TEXT);

    TableMeta employeeMeta = CatalogUtil.newTableMeta(employeeSchema,
        StoreType.CSV);
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = StorageManager.getAppender(conf, employeeMeta, employeePath);
    appender.init();
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < 10; i++) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i),
          DatumFactory.createInt4(i), DatumFactory.createInt4(10 + i),
          DatumFactory.createText("dept_" + i) });
      appender.addTuple(tuple);
    }
    for (int i = 11; i < 20; i+=2) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i),
          DatumFactory.createInt4(i), DatumFactory.createInt4(10 + i),
          DatumFactory.createText("dept_" + i) });
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();
    employee = CatalogUtil.newTableDesc("employee", employeeMeta,
        employeePath);
    catalog.addTable(employee);

    Schema peopleSchema = new Schema();
    peopleSchema.addColumn("empId", Type.INT4);
    peopleSchema.addColumn("fk_memId", Type.INT4);
    peopleSchema.addColumn("name", Type.TEXT);
    peopleSchema.addColumn("age", Type.INT4);
    TableMeta peopleMeta = CatalogUtil.newTableMeta(peopleSchema, StoreType.CSV);
    Path peoplePath = new Path(testDir, "people.csv");
    appender = StorageManager.getAppender(conf, peopleMeta, peoplePath);
    appender.init();
    tuple = new VTuple(peopleMeta.getSchema().getColumnNum());
    for (int i = 1; i < 10; i += 2) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i),
          DatumFactory.createInt4(10 + i),
          DatumFactory.createText("name_" + i),
          DatumFactory.createInt4(30 + i) });
      appender.addTuple(tuple);
    }
    for (int i = 10; i < 20; i++) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i),
          DatumFactory.createInt4(10 + i),
          DatumFactory.createText("name_" + i),
          DatumFactory.createInt4(30 + i) });
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();

    people = CatalogUtil.newTableDesc("people", peopleMeta, peoplePath);
    catalog.addTable(people);
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
  public final void testMergeInnerJoin() throws IOException, PlanningException {
    Fragment[] empFrags = sm.splitNG(conf, "employee", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    Fragment[] peopleFrags = sm.splitNG(conf, "people", people.getMeta(), people.getPath(),
        Integer.MAX_VALUE);

    Fragment[] merged = TUtil.concat(empFrags, peopleFrags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testMergeInnerJoin");
    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        TUtil.newQueryUnitAttemptId(), merged, workDir);
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalNode plan = planner.createPlan(expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;

    // TODO - should be planed with user's optimization hint
    if (!(proj.getChild() instanceof MergeJoinExec)) {
      BinaryPhysicalExec nestedLoopJoin = (BinaryPhysicalExec) proj.getChild();
      SeqScanExec outerScan = (SeqScanExec) nestedLoopJoin.getLeftChild();
      SeqScanExec innerScan = (SeqScanExec) nestedLoopJoin.getRightChild();

      SeqScanExec tmp;
      if (!outerScan.getTableName().equals("employee")) {
        tmp = outerScan;
        outerScan = innerScan;
        innerScan = tmp;
      }

      SortSpec[] outerSortKeys = new SortSpec[2];
      SortSpec[] innerSortKeys = new SortSpec[2];

      Schema employeeSchema = catalog.getTableDesc("employee").getMeta()
          .getSchema();
      outerSortKeys[0] = new SortSpec(
          employeeSchema.getColumnByName("empId"));
      outerSortKeys[1] = new SortSpec(
          employeeSchema.getColumnByName("memId"));
      SortNode outerSort = new SortNode(outerSortKeys);
      outerSort.setInSchema(outerScan.getSchema());
      outerSort.setOutSchema(outerScan.getSchema());

      Schema peopleSchema = catalog.getTableDesc("people").getMeta().getSchema();
      innerSortKeys[0] = new SortSpec(
          peopleSchema.getColumnByName("empId"));
      innerSortKeys[1] = new SortSpec(
          peopleSchema.getColumnByName("fk_memid"));
      SortNode innerSort = new SortNode(innerSortKeys);
      innerSort.setInSchema(innerScan.getSchema());
      innerSort.setOutSchema(innerScan.getSchema());

      MemSortExec outerSortExec = new MemSortExec(ctx, outerSort, outerScan);
      MemSortExec innerSortExec = new MemSortExec(ctx, innerSort, innerScan);

      MergeJoinExec mergeJoin = new MergeJoinExec(ctx,
          ((HashJoinExec)nestedLoopJoin).getPlan(), outerSortExec, innerSortExec,
          outerSortKeys, innerSortKeys);
      proj.setChild(mergeJoin);
      exec = proj;
    }

    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
    while ((tuple = exec.next()) != null) {
      count++;
      assertTrue(i == tuple.getInt(0).asInt4());
      assertTrue(i == tuple.getInt(1).asInt4());
      assertTrue(("dept_" + i).equals(tuple.getString(2).asChars()));
      assertTrue(10 + i == tuple.getInt(3).asInt4());

      i += 2;
    }
    exec.close();
    assertEquals(10, count); // expected 10 * 5
  }
}
