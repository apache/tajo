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
import org.apache.tajo.ExecutionBlockId;
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
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.ExecutionBlockCursor;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TajoWorker;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestDistinctGroupByAggregationExec {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static GlobalPlanner globalPlanner;
  private static AbstractStorageManager sm;
  private static Path testDir;
  //  private static Session session = LocalTajoTestingUtility.createDummySession();
  private static QueryContext defaultContext;

  private static TableDesc employee = null;

  private enum STAGE {
    INIT, FIRST, SECOND, THRID
  }

  ;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();

    util.startCatalogCluster();
    conf = util.getConfiguration();
    testDir = CommonTestingUtil.getTestDir("target/test-data/TestDistinctGroupByAggregationExec");
    sm = StorageManagerFactory.getStorageManager(conf, testDir);
    catalog = util.getMiniCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);
    globalPlanner = new GlobalPlanner(util.getConfiguration(), catalog);

    defaultContext = LocalTajoTestingUtility.createDummyContext(conf);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  String[] QUERIES = {
      "select managerid, count(distinct empid) as cnt1, count(distinct deptname) as cnt2 from employee group by " +
          "managerid ",
      //0
      "select managerid, count(distinct empid) as cnt1, count(distinct deptname) as cnt2, " +
          "count(managerid) as cnt3 from employee group by managerid ", //1
      "select managerid, count(distinct empid) as cnt1, count(distinct deptname) as cnt2, " +
          "count(managerid) as cnt3, count(deptname) as cnt4 from employee group by managerid ", //2
      "select managerid, count(distinct empid) as cnt1, count(distinct deptname) as cnt2, " +
          "count(managerid) as cnt3, count(deptname) as cnt4, sum(managerid) as sum1, sum(empid) as sum2 " +
          "from employee group by managerid " //3
  };

  private void createEmployee(STAGE stage) throws IOException {
    String tableName = CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee");
    Schema employeeSchema = new Schema();

    employeeSchema.addColumn("managerid", Type.INT4);
    employeeSchema.addColumn("empid", Type.INT4);
    employeeSchema.addColumn("deptname", Type.TEXT);

    if (catalog.existsTable(tableName)) {
      catalog.dropTable(tableName);
    }

    TableMeta employeeMeta = CatalogUtil.newTableMeta(StoreType.CSV);

    Path employeePath = new Path(testDir, "employee.csv");

    if (stage == STAGE.INIT) {
      createInitStageData(employeeMeta, employeeSchema, employeePath);
    } else if (stage == STAGE.FIRST) {
      createFirstStageData(employeeMeta, employeeSchema, employeePath);
    }

    employee = new TableDesc(
        CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), employeeSchema, employeeMeta,
        employeePath);
    catalog.createTable(employee);
  }

  private void createInitStageData(TableMeta meta, Schema schema, Path path) throws IOException {
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema,
        path);
    appender.init();
    Tuple tuple = new VTuple(schema.size());
    tuple.put(0, DatumFactory.createInt4(1));
    tuple.put(1, DatumFactory.createInt4(10));
    tuple.put(2, DatumFactory.createText("AA"));
    appender.addTuple(tuple);

    tuple.put(0, DatumFactory.createInt4(2));
    tuple.put(1, DatumFactory.createInt4(15));
    tuple.put(2, DatumFactory.createText("AA"));
    appender.addTuple(tuple);

    tuple.put(0, DatumFactory.createInt4(2));
    tuple.put(1, DatumFactory.createInt4(20));
    tuple.put(2, DatumFactory.createText("BB"));
    appender.addTuple(tuple);

    tuple.put(0, DatumFactory.createInt4(3));
    tuple.put(1, DatumFactory.createInt4(5));
    tuple.put(2, DatumFactory.createText("CC"));
    appender.addTuple(tuple);

    tuple.put(0, DatumFactory.createInt4(4));
    tuple.put(1, DatumFactory.createInt4(5));
    tuple.put(2, DatumFactory.createText("DD"));
    appender.addTuple(tuple);

    tuple.put(0, DatumFactory.createInt4(5));
    tuple.put(1, DatumFactory.createInt4(30));
    tuple.put(2, DatumFactory.createText("EE"));
    appender.addTuple(tuple);
    appender.flush();
    appender.close();
  }

  private void createFirstStageData(TableMeta meta, Schema schema, Path path) throws IOException {
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema,
        path);

    appender.init();

    Tuple tuple = new VTuple(schema.size());

    //1
    tuple.put(0, DatumFactory.createInt4(1));
    tuple.put(1, DatumFactory.createInt4(10));
    tuple.put(2, DatumFactory.createText("AA"));
    appender.addTuple(tuple);

    //2
    tuple.put(0, DatumFactory.createInt4(1));
    tuple.put(1, DatumFactory.createInt4(10));
    tuple.put(2, DatumFactory.createDistinctNullDatum());
    appender.addTuple(tuple);

    //3
    tuple.put(0, DatumFactory.createInt4(1));
    tuple.put(1, DatumFactory.createDistinctNullDatum());
    tuple.put(2, DatumFactory.createText("AA"));
    appender.addTuple(tuple);

    //4
    tuple.put(0, DatumFactory.createInt4(2));
    tuple.put(1, DatumFactory.createInt4(15));
    tuple.put(2, DatumFactory.createText("AA"));
    appender.addTuple(tuple);

    //5
    tuple.put(0, DatumFactory.createInt4(2));
    tuple.put(1, DatumFactory.createInt4(15));
    tuple.put(2, DatumFactory.createDistinctNullDatum());
    appender.addTuple(tuple);

    //6
    tuple.put(0, DatumFactory.createInt4(2));
    tuple.put(1, DatumFactory.createDistinctNullDatum());
    tuple.put(2, DatumFactory.createText("AA"));
    appender.addTuple(tuple);

    //7
    tuple.put(0, DatumFactory.createInt4(2));
    tuple.put(1, DatumFactory.createInt4(20));
    tuple.put(2, DatumFactory.createText("BB"));
    appender.addTuple(tuple);

    //8
    tuple.put(0, DatumFactory.createInt4(2));
    tuple.put(1, DatumFactory.createInt4(20));
    tuple.put(2, DatumFactory.createDistinctNullDatum());
    appender.addTuple(tuple);

    //9
    tuple.put(0, DatumFactory.createInt4(2));
    tuple.put(1, DatumFactory.createDistinctNullDatum());
    tuple.put(2, DatumFactory.createText("BB"));
    appender.addTuple(tuple);

    //10
    tuple.put(0, DatumFactory.createInt4(3));
    tuple.put(1, DatumFactory.createInt4(5));
    tuple.put(2, DatumFactory.createText("CC"));
    appender.addTuple(tuple);

    //11
    tuple.put(0, DatumFactory.createInt4(3));
    tuple.put(1, DatumFactory.createInt4(5));
    tuple.put(2, DatumFactory.createDistinctNullDatum());
    appender.addTuple(tuple);

    //12
    tuple.put(0, DatumFactory.createInt4(3));
    tuple.put(1, DatumFactory.createDistinctNullDatum());
    tuple.put(2, DatumFactory.createText("CC"));
    appender.addTuple(tuple);

    //13
    tuple.put(0, DatumFactory.createInt4(4));
    tuple.put(1, DatumFactory.createInt4(5));
    tuple.put(2, DatumFactory.createText("DD"));
    appender.addTuple(tuple);

    //14
    tuple.put(0, DatumFactory.createInt4(4));
    tuple.put(1, DatumFactory.createInt4(5));
    tuple.put(2, DatumFactory.createDistinctNullDatum());
    appender.addTuple(tuple);

    //15
    tuple.put(0, DatumFactory.createInt4(4));
    tuple.put(1, DatumFactory.createDistinctNullDatum());
    tuple.put(2, DatumFactory.createText("DD"));
    appender.addTuple(tuple);

    //16
    tuple.put(0, DatumFactory.createInt4(5));
    tuple.put(1, DatumFactory.createInt4(30));
    tuple.put(2, DatumFactory.createText("EE"));
    appender.addTuple(tuple);

    //17
    tuple.put(0, DatumFactory.createInt4(5));
    tuple.put(1, DatumFactory.createInt4(30));
    tuple.put(2, DatumFactory.createDistinctNullDatum());
    appender.addTuple(tuple);

    //18
    tuple.put(0, DatumFactory.createInt4(5));
    tuple.put(1, DatumFactory.createDistinctNullDatum());
    tuple.put(2, DatumFactory.createText("EE"));
    appender.addTuple(tuple);

    appender.flush();
    appender.close();
  }

  private ExecutionBlockId getRootBlock(MasterPlan masterPlan) throws PlanningException {
    ExecutionBlockCursor cursor = new ExecutionBlockCursor(masterPlan, true);

    ExecutionBlockId executionBlockId = null;
    if (cursor.hasNext()) {
      ExecutionBlock executionBlock = cursor.nextBlock();
      executionBlockId = executionBlock.getId();
    }

    return executionBlockId;
  }

  @Test
  public final void testIntermediateAggregation() throws IOException, PlanningException {
    String tableName = "default.employee";

    // Create first stage result data in force
    createEmployee(STAGE.FIRST);

    // Setting file path
    FileFragment[] frags = StorageManager.splitNG(conf, tableName, employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testIntermediateAggregation");

    for (int index = 0; index < QUERIES.length; index++) {

      // Build global plan
      Expr expr = analyzer.parse(QUERIES[index]);
      LogicalPlan plan = planner.createPlan(defaultContext, expr);
      optimizer.optimize(plan);
      QueryContext context = new QueryContext(util.getConfiguration());
      MasterPlan masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), context, plan);
      globalPlanner.build(masterPlan);

      // Verify input data
      Scanner scanner = StorageManagerFactory.getStorageManager(conf).getFileScanner(employee.getMeta(),
          employee.getSchema(), employee.getPath());
      scanner.init();

      int i = 0;
      Tuple tuple = null;
      int firstDistinctColumnCount = 0, secondDistinctColumnCount = 0;
      while ((tuple = scanner.next()) != null) {
        if (tuple.isDistinctNull(1)) {
          firstDistinctColumnCount++;
        }
        if (tuple.isDistinctNull(2)) {
          secondDistinctColumnCount++;
        }
        i++;
      }

      assertEquals(i, 18);
      assertEquals(firstDistinctColumnCount, secondDistinctColumnCount);
      assertEquals(firstDistinctColumnCount, 6);
      assertEquals(secondDistinctColumnCount, 6);

      // Get ExecutionBlockId for creating PhysicalPlanner
      ExecutionBlockId executionBlockId = getRootBlock(masterPlan);
      assertNotNull(executionBlockId);

      ExecutionBlock executionBlock = masterPlan.getExecBlock(executionBlockId);
      assertEquals(executionBlock.getPlan().getType(), NodeType.DISTINCT_GROUP_BY);

      DistinctGroupbyNode distinctGroupbyNode = (DistinctGroupbyNode) executionBlock.getPlan();

      TaskAttemptContext ctx = new TaskAttemptContext(context,
          LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
          new FileFragment[]{frags[0]}, workDir);
      ctx.setEnforcer(new Enforcer());
//    ctx.getEnforcer().enforceDistinctAggregation(distinctGroupbyNode.getPID(),
//        TajoWorkerProtocol.DistinctGroupbyEnforcer.DistinctAggregationAlgorithm.HASH_AGGREGATION, null);

      PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
      PhysicalExec exec = phyPlanner.createPlan(ctx, distinctGroupbyNode);

      assertEquals(exec.getClass().getCanonicalName(), DistinctGroupbyHashAggregationExec.class.getCanonicalName());
      DistinctGroupbyHashAggregationExec hashAggregationExec = (DistinctGroupbyHashAggregationExec) exec;

      SeqScanExec scanLeftChild = (SeqScanExec) hashAggregationExec.getChild();
      assertEquals(scanLeftChild.getTableName(), tableName);

      DistinctGroupbyIntermediateAggregationExec intermediateAggregationExec = new
          DistinctGroupbyIntermediateAggregationExec(ctx, distinctGroupbyNode, scanLeftChild);

      i = 0;
      intermediateAggregationExec.init();
      while ((tuple = intermediateAggregationExec.next()) != null) {
        if (index == 0) {
          if (tuple.getInt4(0) == 1) {
            assertEquals(tuple.get(1).asInt4(), 1);
            assertEquals(tuple.get(2).asInt4(), 1);
          } else if (tuple.getInt4(0) == 2) {
            assertEquals(tuple.get(1).asInt4(), 2);
            assertEquals(tuple.get(2).asInt4(), 2);
          }
        } else if (index == 1) {
          if (tuple.getInt4(0) == 1) {
            assertEquals(tuple.get(1).asInt4(), 1);
            assertEquals(tuple.get(2).asInt4(), 1);
            assertEquals(tuple.get(3).asInt4(), 1);
          } else if (tuple.getInt4(0) == 2) {
            assertEquals(tuple.get(1).asInt4(), 2);
            assertEquals(tuple.get(2).asInt4(), 2);
            assertEquals(tuple.get(3).asInt4(), 2);
          }
        } else if (index == 2) {
          if (tuple.getInt4(0) == 1) {
            assertEquals(tuple.get(1).asInt4(), 1);
            assertEquals(tuple.get(2).asInt4(), 1);
            assertEquals(tuple.get(3).asInt4(), 1);
            assertEquals(tuple.get(4).asInt4(), 1);
          } else if (tuple.getInt4(0) == 2) {
            assertEquals(tuple.get(1).asInt4(), 2);
            assertEquals(tuple.get(2).asInt4(), 2);
            assertEquals(tuple.get(3).asInt4(), 2);
            assertEquals(tuple.get(4).asInt4(), 2);
          }
        } else if (index == 3) {
          if (tuple.getInt4(0) == 1) {
            assertEquals(tuple.get(1).asInt4(), 1);
            assertEquals(tuple.get(2).asInt4(), 1);
            assertEquals(tuple.get(3).asInt4(), 1);
            assertEquals(tuple.get(4).asInt4(), 1);
            assertEquals(tuple.get(5).asInt4(), 1);
            assertEquals(tuple.get(6).asInt4(), 10);
          } else if (tuple.getInt4(0) == 2) {
            assertEquals(tuple.get(1).asInt4(), 2);
            assertEquals(tuple.get(2).asInt4(), 2);
            assertEquals(tuple.get(3).asInt4(), 2);
            assertEquals(tuple.get(4).asInt4(), 2);
            assertEquals(tuple.get(5).asInt4(), 4);
            assertEquals(tuple.get(6).asInt4(), 35);
          } else if (tuple.getInt4(0) == 3) {
            assertEquals(tuple.get(1).asInt4(), 1);
            assertEquals(tuple.get(2).asInt4(), 1);
            assertEquals(tuple.get(3).asInt4(), 1);
            assertEquals(tuple.get(4).asInt4(), 1);
            assertEquals(tuple.get(5).asInt4(), 3);
            assertEquals(tuple.get(6).asInt4(), 5);
          }
        }
        i++;
      }
      intermediateAggregationExec.close();
      assertEquals(i, 5);
    }
  }

}
