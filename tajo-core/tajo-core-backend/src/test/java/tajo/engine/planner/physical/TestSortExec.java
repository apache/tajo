/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.planner.physical;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.TajoTestingCluster;
import tajo.TaskAttemptContext;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.*;
import tajo.engine.planner.logical.LogicalNode;
import tajo.storage.*;
import tajo.util.CommonTestingUtil;
import tajo.util.TUtil;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class TestSortExec {
  private static TajoConf conf;
  private static final String TEST_PATH = "target/test-data/TestPhysicalPlanner";
  private static CatalogService catalog;
  private static QueryAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static StorageManager sm;
  private static TajoTestingCluster util;
  private static Path workDir;
  private static Path tablePath;
  private static TableMeta employeeMeta;

  private static Random rnd = new Random(System.currentTimeMillis());

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new TajoConf();
    util = new TajoTestingCluster();
    catalog = util.startCatalogCluster().getCatalog();
    workDir = CommonTestingUtil.getTestDir(TEST_PATH);
    sm = StorageManager.get(conf, workDir);

    Schema schema = new Schema();
    schema.addColumn("managerId", DataType.INT);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);

    employeeMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    tablePath = StorageUtil.concatPath(workDir, "employee", "table1");
    sm.getFileSystem().mkdirs(tablePath.getParent());

    Appender appender = StorageManager.getAppender(conf, employeeMeta, tablePath);
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < 100; i++) {
      tuple.put(new Datum[] {
          DatumFactory.createInt(rnd.nextInt(5)),
          DatumFactory.createInt(rnd.nextInt(10)),
          DatumFactory.createString("dept_" + rnd.nextInt(10))});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    TableDesc desc = new TableDescImpl("employee", employeeMeta, tablePath);
    catalog.addTable(desc);

    analyzer = new QueryAnalyzer(catalog);
    planner = new LogicalPlanner(catalog);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  public static String[] QUERIES = {
      "select managerId, empId, deptName from employee order by managerId, empId desc" };

  @Test
  public final void testNext() throws IOException {
    Fragment [] frags = sm.splitNG(conf, "employee", employeeMeta, tablePath, Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/TestSortExec");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil
        .newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    PlanningContext context = analyzer.parse(QUERIES[0]);
    LogicalNode plan = planner.createPlan(context);
    LogicalOptimizer.optimize(context, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

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
    schema.addColumn("l_orderkey", DataType.LONG);
    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createLong(0));
    Tuple e = new VTuple(1);
    e.put(0, DatumFactory.createLong(6000000000l));
    TupleRange expected = new TupleRange(schema, s, e);
    RangePartitionAlgorithm partitioner
        = new UniformRangePartition(schema, expected, true);
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
