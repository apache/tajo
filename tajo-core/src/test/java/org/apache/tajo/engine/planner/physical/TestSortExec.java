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
import org.apache.tajo.TpchTestBase;
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
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class TestSortExec {
  private static TajoConf conf;
  private static final String TEST_PATH = "target/test-data/TestPhysicalPlanner";
  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static AbstractStorageManager sm;
  private static Path workDir;
  private static Path tablePath;
  private static TableMeta employeeMeta;

  private static Random rnd = new Random(System.currentTimeMillis());

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new TajoConf();
    util = TpchTestBase.getInstance().getTestingCluster();
    catalog = util.getMaster().getCatalog();
    workDir = CommonTestingUtil.getTestDir(TEST_PATH);
    sm = StorageManagerFactory.getStorageManager(conf, workDir);

    Schema schema = new Schema();
    schema.addColumn("managerid", Type.INT4);
    schema.addColumn("empid", Type.INT4);
    schema.addColumn("deptname", Type.TEXT);

    employeeMeta = CatalogUtil.newTableMeta(StoreType.CSV);

    tablePath = StorageUtil.concatPath(workDir, "employee", "table1");
    sm.getFileSystem().mkdirs(tablePath.getParent());

    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(employeeMeta, schema, tablePath);
    appender.init();
    Tuple tuple = new VTuple(schema.size());
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
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, employeeMeta,
        tablePath);
    catalog.createTable(desc);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);
  }

  public static String[] QUERIES = {
      "select managerId, empId, deptName from employee order by managerId, empId desc" };

  @Test
  public final void testNext() throws IOException, PlanningException {
    FileFragment[] frags = StorageManager.splitNG(conf, "default.employee", employeeMeta, tablePath, Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/TestSortExec");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility
        .newQueryUnitAttemptId(), new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), context);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    Tuple tuple;
    Datum preVal = null;
    Datum curVal;
    exec.init();
    while ((tuple = exec.next()) != null) {
      curVal = tuple.get(0);
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
    Schema schema = new Schema();
    schema.addColumn("l_orderkey", Type.INT8);
    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createInt8(0));
    Tuple e = new VTuple(1);
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
