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
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertNotNull;
import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestProgressExternalSortExec {
  private TajoConf conf;
  private TajoTestingCluster util;
  private final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestProgressExternalSortExec";
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private Path testDir;

  private final int numTuple = 50000;
  private Random rnd = new Random(System.currentTimeMillis());

  private TableDesc employee;

  private TableStats testDataStats;
  @Before
  public void setUp() throws Exception {
    this.conf = new TajoConf();
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getCatalogService();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    conf.setVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR, testDir.toString());

    Schema schema = SchemaBuilder.builder()
        .add("managerid", TajoDataTypes.Type.INT4)
        .add("empid", TajoDataTypes.Type.INT4)
        .add("deptname", TajoDataTypes.Type.TEXT)
        .build();

    TableMeta employeeMeta = CatalogUtil.newTableMeta(BuiltinStorages.RAW, conf);
    Path employeePath = new Path(testDir, "employee.raw");
    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs())
        .getAppender(employeeMeta, schema, employeePath);
    appender.enableStats();
    appender.init();
    VTuple tuple = new VTuple(schema.size());
    for (int i = 0; i < numTuple; i++) {
      tuple.put(new Datum[] {
          DatumFactory.createInt4(rnd.nextInt(50)),
          DatumFactory.createInt4(rnd.nextInt(100)),
          DatumFactory.createText("dept_" + i),
      });
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    testDataStats = appender.getStats();
    employee = new TableDesc(
        IdentifierUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, employeeMeta,
        employeePath.toUri());
    catalog.createTable(employee);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    employeePath.getFileSystem(conf).deleteOnExit(employeePath);
  }

  @After
  public void tearDown() throws Exception {
    CommonTestingUtil.cleanupTestDir(TEST_PATH);
    util.shutdownCatalogCluster();
  }

  String[] QUERIES = {
      "select managerId, empId from employee order by managerId, empId"
  };

  @Test
  public void testExternalSortExecProgressWithMemTableScanner() throws Exception {
    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    int bufferSize = (int) (testDataStats.getNumBytes() * 2) / StorageUnit.MB; //multiply 2 for memory fit
    queryContext.setInt(SessionVars.EXTSORT_BUFFER_SIZE, bufferSize);
    testProgress(queryContext);
  }

  @Test
  public void testExternalSortExecProgressWithPairWiseMerger() throws Exception {
    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    int bufferSize = (int) Math.max((testDataStats.getNumBytes() / StorageUnit.MB), 1);
    queryContext.setInt(SessionVars.EXTSORT_BUFFER_SIZE, bufferSize);

    testProgress(queryContext);
  }

  private void testProgress(QueryContext queryContext) throws Exception {
    conf.setIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT, 2);

    FileFragment[] frags = FileTablespace.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getUri()), Integer.MAX_VALUE);
    Path workDir = new Path(testDir, TestExternalSortExec.class.getName());
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext,
        LocalTajoTestingUtility.newTaskAttemptId(), new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), expr);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    ProjectionExec proj = (ProjectionExec) exec;
    Tuple tuple;
    Tuple preVal = null;
    Tuple curVal;
    int cnt = 0;
    exec.init();
    BaseTupleComparator comparator = new BaseTupleComparator(proj.getSchema(),
        new SortSpec[]{
            new SortSpec(new Column("managerid", TajoDataTypes.Type.INT4)),
            new SortSpec(new Column("empid", TajoDataTypes.Type.INT4))
        });

    float initProgress = 0.0f;
    while ((tuple = exec.next()) != null) {
      if (cnt == 0) {
        initProgress = exec.getProgress();
        assertTrue(initProgress > 0.5f && initProgress < 1.0f);
      }

      if (cnt == testDataStats.getNumRows() / 2) {
        float progress = exec.getProgress();
        assertTrue(progress > initProgress);
      }
      curVal = tuple;
      if (preVal != null) {
        assertTrue("prev: " + preVal + ", but cur: " + curVal, comparator.compare(preVal, curVal) <= 0);
      }
      preVal = curVal;
      cnt++;
    }

    assertEquals(1.0f, exec.getProgress(), 0);
    assertEquals(numTuple, cnt);

    TableStats tableStats = exec.getInputStats();
    assertNotNull(tableStats);
    assertEquals(testDataStats.getNumBytes().longValue(), tableStats.getNumBytes().longValue());
    assertEquals(testDataStats.getNumRows().longValue(), cnt);
    assertEquals(testDataStats.getNumRows().longValue(), tableStats.getNumRows().longValue());
    assertTrue(testDataStats.getNumBytes() <= tableStats.getReadBytes());

    // for rescan test
    preVal = null;
    exec.rescan();

    cnt = 0;
    while ((tuple = exec.next()) != null) {
      curVal = tuple;
      if (preVal != null) {
        assertTrue("prev: " + preVal + ", but cur: " + curVal, comparator.compare(preVal, curVal) <= 0);
      }
      preVal = curVal;
      cnt++;
    }
    assertEquals(1.0f, exec.getProgress(), 0);
    assertEquals(numTuple, cnt);
    exec.close();
    assertEquals(1.0f, exec.getProgress(), 0);

    tableStats = exec.getInputStats();
    assertNotNull(tableStats);
    assertEquals(testDataStats.getNumBytes().longValue(), tableStats.getNumBytes().longValue());
    assertEquals(testDataStats.getNumRows().longValue(), cnt);
    assertEquals(testDataStats.getNumRows().longValue(), tableStats.getNumRows().longValue());
    //'ReadBytes' is actual read bytes
    assertTrue(testDataStats.getNumBytes() <= tableStats.getReadBytes());

    conf.setIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT, ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT.defaultIntVal);
  }
}
