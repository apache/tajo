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
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.RangePartitionAlgorithm;
import org.apache.tajo.engine.planner.UniformRangePartition;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class TestSortExec {
  private static TajoConf conf;
  private static final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestPhysicalPlanner";
  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static FileTablespace sm;
  private static Path workDir;
  private static Path tablePath;
  private static TableMeta employeeMeta;
  private static QueryContext queryContext;

  private static Random rnd = new Random(System.currentTimeMillis());

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new TajoConf();
    conf.setBoolVar(TajoConf.ConfVars.$TEST_MODE, true);
    conf.setIntVar(ConfVars.$SORT_LIST_SIZE, 100);
    util = TpchTestBase.getInstance().getTestingCluster();
    catalog = util.getMaster().getCatalog();
    workDir = CommonTestingUtil.getTestDir(TEST_PATH);
    sm = TablespaceManager.getLocalFs();

    Schema schema = SchemaBuilder.builder()
        .add("managerid", Type.INT4)
        .add("empid", Type.INT4)
        .add("deptname", Type.TEXT)
        .build();

    employeeMeta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration());

    tablePath = StorageUtil.concatPath(workDir, "employee", "table1");
    sm.getFileSystem().mkdirs(tablePath.getParent());

    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs())
        .getAppender(employeeMeta, schema, tablePath);
    appender.init();
    VTuple tuple = new VTuple(schema.size());
    for (int i = 0; i < 100; i++) {
      tuple.put(new Datum[] {
          DatumFactory.createInt4(rnd.nextInt(5)),
          DatumFactory.createInt4(rnd.nextInt(10)),
          DatumFactory.createText("dept_" + rnd.nextInt(10))});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    TableDesc desc = new TableDesc(
        IdentifierUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, employeeMeta,
        tablePath.toUri());
    catalog.createTable(desc);

    queryContext = new QueryContext(conf);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(conf, catalog, TablespaceManager.getInstance());
  }

  public static String[] QUERIES = {
      "select managerId, empId, deptName from employee order by managerId, empId desc" };

  @Test
  public final void testNext() throws IOException, TajoException {
    FileFragment[] frags = FileTablespace.splitNG(conf, "default.employee", employeeMeta, tablePath, Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestSortExec");
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext,
        LocalTajoTestingUtility
        .newTaskAttemptId(), new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(conf), context);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    Tuple tuple;
    Datum preVal = null;
    Datum curVal;
    exec.init();
    while ((tuple = exec.next()) != null) {
      curVal = tuple.asDatum(0);
      if (preVal != null) {
        assertTrue(preVal.lessThanEqual(curVal).asBool());
      }

      preVal = curVal;
    }
    exec.close();
  }

  @Test
  /**
   * TODO - Now, in FSM branch, TestUniformRangePartition is ported to Java.
   * So, this test is located in here.
   * Later it should be moved TestUniformPartitions.
   */
  public void testTAJO_946() {
    Schema schema = SchemaBuilder.builder().add("l_orderkey", Type.INT8).build();
    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    s.put(0, DatumFactory.createInt8(0));
    VTuple e = new VTuple(1);
    e.put(0, DatumFactory.createInt8(6000000000l));
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner
        = new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(967);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) < 0);
      }
    }
  }
}
