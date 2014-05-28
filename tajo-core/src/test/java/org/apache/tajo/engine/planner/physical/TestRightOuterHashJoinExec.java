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
import org.apache.tajo.engine.planner.logical.LogicalNode;
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

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;

// this is not a physical operator in itself, but it uses the HashLeftOuterJoinExec with switched inputs order
public class TestRightOuterHashJoinExec {
  private TajoConf conf;
  private final String TEST_PATH = "target/test-data/TestRightOuterHashJoinExec";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private AbstractStorageManager sm;
  private Path testDir;
  private static Session session = LocalTajoTestingUtility.createDummySession();

  private TableDesc dep3;
  private TableDesc job3;
  private TableDesc emp3;

  private final String DEP3_NAME = CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "dep3");
  private final String JOB3_NAME = CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "job3");
  private final String EMP3_NAME = CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "emp3");

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

    //----------------- dep3 ------------------------------
    // dep_id | dep_name  | loc_id
    //--------------------------------
    //  0     | dep_0     | 1000
    //  1     | dep_1     | 1001
    //  2     | dep_2     | 1002
    //  3     | dep_3     | 1003
    //  4     | dep_4     | 1004
    //  5     | dep_5     | 1005
    //  6     | dep_6     | 1006
    //  7     | dep_7     | 1007
    //  8     | dep_8     | 1008
    //  9     | dep_9     | 1009
    Schema dep3Schema = new Schema();
    dep3Schema.addColumn("dep_id", Type.INT4);
    dep3Schema.addColumn("dep_name", Type.TEXT);
    dep3Schema.addColumn("loc_id", Type.INT4);


    TableMeta dep3Meta = CatalogUtil.newTableMeta(StoreType.CSV);
    Path dep3Path = new Path(testDir, "dep3.csv");
    Appender appender1 = StorageManagerFactory.getStorageManager(conf).getAppender(dep3Meta, dep3Schema, dep3Path);
    appender1.init();
    Tuple tuple = new VTuple(dep3Schema.size());
    for (int i = 0; i < 10; i++) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i),
                    DatumFactory.createText("dept_" + i),
                    DatumFactory.createInt4(1000 + i) });
      appender1.addTuple(tuple);
    }

    appender1.flush();
    appender1.close();
    dep3 = CatalogUtil.newTableDesc(DEP3_NAME, dep3Schema, dep3Meta, dep3Path);
    catalog.createTable(dep3);

    //----------------- job3 ------------------------------
    //  job_id  | job_title
    // ----------------------
    //   101    |  job_101
    //   102    |  job_102
    //   103    |  job_103

    Schema job3Schema = new Schema();
    job3Schema.addColumn("job_id", Type.INT4);
    job3Schema.addColumn("job_title", Type.TEXT);


    TableMeta job3Meta = CatalogUtil.newTableMeta(StoreType.CSV);
    Path job3Path = new Path(testDir, "job3.csv");
    Appender appender2 = StorageManagerFactory.getStorageManager(conf).getAppender(job3Meta, job3Schema, job3Path);
    appender2.init();
    Tuple tuple2 = new VTuple(job3Schema.size());
    for (int i = 1; i < 4; i++) {
      int x = 100 + i;
      tuple2.put(new Datum[] { DatumFactory.createInt4(100 + i),
                    DatumFactory.createText("job_" + x) });
      appender2.addTuple(tuple2);
    }

    appender2.flush();
    appender2.close();
    job3 = CatalogUtil.newTableDesc(JOB3_NAME, job3Schema, job3Meta, job3Path);
    catalog.createTable(job3);



    //---------------------emp3 --------------------
    // emp_id  | first_name | last_name | dep_id | salary | job_id
    // ------------------------------------------------------------
    //  11     |  fn_11     |  ln_11    |  1     | 123    | 101
    //  13     |  fn_13     |  ln_13    |  3     | 369    | 103
    //  15     |  fn_15     |  ln_15    |  5     | 615    | null
    //  17     |  fn_17     |  ln_17    |  7     | 861    | null
    //  19     |  fn_19     |  ln_19    |  9     | 1107   | null
    //  21     |  fn_21     |  ln_21    |  1     | 123    | 101
    //  23     |  fn_23     |  ln_23    |  3     | 369    | 103

    Schema emp3Schema = new Schema();
    emp3Schema.addColumn("emp_id", Type.INT4);
    emp3Schema.addColumn("first_name", Type.TEXT);
    emp3Schema.addColumn("last_name", Type.TEXT);
    emp3Schema.addColumn("dep_id", Type.INT4);
    emp3Schema.addColumn("salary", Type.FLOAT4);
    emp3Schema.addColumn("job_id", Type.INT4);


    TableMeta emp3Meta = CatalogUtil.newTableMeta(StoreType.CSV);
    Path emp3Path = new Path(testDir, "emp3.csv");
    Appender appender3 = StorageManagerFactory.getStorageManager(conf).getAppender(emp3Meta, emp3Schema, emp3Path);
    appender3.init();
    Tuple tuple3 = new VTuple(emp3Schema.size());

    for (int i = 1; i < 4; i += 2) {
      int x = 10 + i;
      tuple3.put(new Datum[] { DatumFactory.createInt4(10 + i),
          DatumFactory.createText("firstname_" + x),
          DatumFactory.createText("lastname_" + x),
          DatumFactory.createInt4(i),
          DatumFactory.createFloat4(123 * i),
          DatumFactory.createInt4(100 + i) });
      appender3.addTuple(tuple3);

      int y = 20 + i;
      tuple3.put(new Datum[] { DatumFactory.createInt4(20 + i),
          DatumFactory.createText("firstname_" + y),
          DatumFactory.createText("lastname_" + y),
          DatumFactory.createInt4(i),
          DatumFactory.createFloat4(123 * i),
          DatumFactory.createInt4(100 + i) });
      appender3.addTuple(tuple3);
    }

    for (int i = 5; i < 10; i += 2) {
      int x = 10 + i;
      tuple3.put(new Datum[] { DatumFactory.createInt4(10 + i),
          DatumFactory.createText("firstname_" + x),
          DatumFactory.createText("lastname_" + x),
          DatumFactory.createInt4(i),
          DatumFactory.createFloat4(123 * i),
          DatumFactory.createNullDatum() });
      appender3.addTuple(tuple3);
    }

    appender3.flush();
    appender3.close();
    emp3 = CatalogUtil.newTableDesc(EMP3_NAME, emp3Schema, emp3Meta, emp3Path);
    catalog.createTable(emp3);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  String[] QUERIES = {
      "select dep3.dep_id, dep_name, emp_id, salary from emp3 right outer join dep3 on dep3.dep_id = emp3.dep_id", //0 no nulls
      "select job3.job_id, job_title, emp_id, salary from emp3 right outer join job3 on job3.job_id=emp3.job_id", //1 nulls on the left operand
      "select job3.job_id, job_title, emp_id, salary from job3 right outer join emp3 on job3.job_id=emp3.job_id" //2 nulls on the right side
  };

  @Test
  public final void testRightOuter_HashJoinExec0() throws IOException, PlanningException {
    FileFragment[] emp3Frags = StorageManager.splitNG(conf, EMP3_NAME, emp3.getMeta(), emp3.getPath(),
        Integer.MAX_VALUE);
    FileFragment[] dep3Frags = StorageManager.splitNG(conf, DEP3_NAME, dep3.getMeta(), dep3.getPath(),
        Integer.MAX_VALUE);

    FileFragment[] merged = TUtil.concat(emp3Frags, dep3Frags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/TestRightOuter_HashJoinExec0");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(), merged, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalNode plan = planner.createPlan(session, expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;
    if (proj.getChild() instanceof RightOuterMergeJoinExec) {
       //for this small data set this is not likely to happen
      
      assertEquals(1, 0);
    }
    else{
       Tuple tuple;
       int count = 0;
       int i = 1;
       exec.init();
  
       while ((tuple = exec.next()) != null) {
         //TODO check contents
         count = count + 1;
       }
       exec.close();
       assertEquals(12, count);
    }
  }


  @Test
  public final void testRightOuter_HashJoinExec1() throws IOException, PlanningException {
    FileFragment[] job3Frags = StorageManager.splitNG(conf, JOB3_NAME, job3.getMeta(), job3.getPath(),
        Integer.MAX_VALUE);
    FileFragment[] emp3Frags = StorageManager.splitNG(conf, EMP3_NAME, emp3.getMeta(), emp3.getPath(),
        Integer.MAX_VALUE);

    FileFragment[] merged = TUtil.concat(emp3Frags, job3Frags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/TestRightOuter_HashJoinExec1");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(), merged, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[1]);
    LogicalNode plan = planner.createPlan(session, expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;
    if (proj.getChild() instanceof RightOuterMergeJoinExec) {
       //for this small data set this is not likely to happen
      
      assertEquals(1, 0);
    }
    else{
       Tuple tuple;
       int count = 0;
       int i = 1;
       exec.init();
  
       while ((tuple = exec.next()) != null) {
         //TODO check contents
         count = count + 1;
       }
       exec.close();
       assertEquals(5, count);
    }
  }

    @Test
  public final void testRightOuter_HashJoinExec2() throws IOException, PlanningException {
    
    FileFragment[] emp3Frags = StorageManager.splitNG(conf, EMP3_NAME, emp3.getMeta(), emp3.getPath(),
        Integer.MAX_VALUE);
    FileFragment[] job3Frags = StorageManager.splitNG(conf, JOB3_NAME, job3.getMeta(), job3.getPath(),
        Integer.MAX_VALUE);

    FileFragment[] merged = TUtil.concat(job3Frags, emp3Frags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/TestRightOuter_HashJoinExec2");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(), merged, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[2]);
    LogicalNode plan = planner.createPlan(session, expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;
    if (proj.getChild() instanceof RightOuterMergeJoinExec) {
      //for this small data set this is not likely to happen
      
      assertEquals(1, 0);
    }
    else{
       Tuple tuple;
       int count = 0;
       int i = 1;
       exec.init();
  
       while ((tuple = exec.next()) != null) {
         //TODO check contents
         count = count + 1;
       }
       exec.close();
       assertEquals(7, count);
    }
  }
}
