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
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.physical.ExternalSortExec.SortAlgorithm;
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
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestExternalSortExec {
  private TajoConf conf;
  private TajoTestingCluster util;
  private final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestExternalSortExec";
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private Path testDir;
  private Schema tableSchema;

  private final int numTuple = 1000;
  private Random rnd = new Random(System.currentTimeMillis());

  private TableDesc employee;
  private String sortAlgorithmString;

  public TestExternalSortExec(String sortAlgorithm) {
    this.sortAlgorithmString = sortAlgorithm;
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {SortAlgorithm.TIM.name()},
        {SortAlgorithm.MSD_RADIX.name()},
    });
  }

  @Before
  public void setUp() throws Exception {
    this.conf = new TajoConf();
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getCatalogService();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    conf.setVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR, testDir.toString());

    tableSchema = SchemaBuilder.builder().addAll(new Column[] {
        new Column("managerid", Type.INT8),
        new Column("empid", Type.INT4),
        new Column("deptname", Type.TEXT),
        new Column("col1", Type.INT8),
        new Column("col2", Type.INT8),
        new Column("col3", Type.INT8),
        new Column("col4", Type.INT8),
        new Column("col5", Type.INT8),
        new Column("col6", Type.INT8),
        new Column("col7", Type.INT8),
        new Column("col8", Type.INT8),
        new Column("col9", Type.INT8),
        new Column("col10", Type.INT8),
        new Column("col11", Type.INT8),
        new Column("col12", Type.INT8)
    }).build();

    TableMeta employeeMeta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration());
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs())
        .getAppender(employeeMeta, tableSchema, employeePath);
    appender.enableStats();
    appender.init();
    VTuple tuple = new VTuple(tableSchema.size());
    for (int i = 0; i < numTuple; i++) {
      if (rnd.nextInt(1000) == 0) {
        tuple.put(new Datum[] {
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
            NullDatum.get(),
        });
      } else {
        boolean positive = rnd.nextInt(2) == 0;
        tuple.put(new Datum[]{
            DatumFactory.createInt8(positive ? 100_000 + rnd.nextInt(100_000) : (100_000 + rnd.nextInt(100_000)) * -1),
            DatumFactory.createInt4(rnd.nextInt(100)),
            DatumFactory.createText("dept_" + i),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
            DatumFactory.createInt8(100_000 + rnd.nextInt(50)),
        });
      }
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();

    employee = new TableDesc("default.employee", tableSchema, employeeMeta, employeePath.toUri());
    catalog.createTable(employee);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(conf, catalog, TablespaceManager.getInstance());
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
  public final void testNext() throws IOException, TajoException {
    conf.setIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT, 2);
    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    queryContext.set(SessionVars.SORT_ALGORITHM.keyname(), sortAlgorithmString);
    queryContext.setInt(SessionVars.EXTSORT_BUFFER_SIZE, 4);

    FileFragment[] frags = FileTablespace.splitNG(conf, "default.employee", employee.getMeta(),
        new Path(employee.getUri()), Integer.MAX_VALUE);
    Path workDir = new Path(testDir, TestExternalSortExec.class.getName());
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext,
        LocalTajoTestingUtility.newTaskAttemptId(), new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    
    Tuple tuple;
    Tuple preVal = null;
    Tuple curVal;
    int cnt = 0;
    exec.init();
    Schema sortSchema = SchemaBuilder.builder().addAll(new Column[] {
        new Column("managerid", Type.INT8),
        new Column("empid", Type.INT4),
    }).build();

    BaseTupleComparator comparator = new BaseTupleComparator(sortSchema,
        new SortSpec[]{
            new SortSpec(new Column("managerid", Type.INT8)),
            new SortSpec(new Column("empid", Type.INT4)),
        });

    long start = System.currentTimeMillis();
    while ((tuple = exec.next()) != null) {
      curVal = tuple;
      if (preVal != null) {
        assertTrue("prev: " + preVal + ", but cur: " + curVal + ", cnt: " + cnt, comparator.compare(preVal, curVal) <= 0);
      }
      preVal = new VTuple(curVal);
      cnt++;
    }
    long end = System.currentTimeMillis();
    assertEquals(numTuple, cnt);

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
    assertEquals(numTuple, cnt);
    exec.close();
    System.out.println("Sort Time: " + (end - start) + " msc");
    conf.setIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT, ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT.defaultIntVal);
  }
}
