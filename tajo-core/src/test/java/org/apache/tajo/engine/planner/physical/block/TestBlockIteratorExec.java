/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.engine.planner.physical.block;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.physical.*;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.GlobalEngine;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.tuple.RowBlockReader;
import org.apache.tajo.tuple.offheap.OffHeapRowBlock;
import org.apache.tajo.tuple.offheap.ZeroCopyTuple;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestBlockIteratorExec extends QueryTestCaseBase {

  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static PhysicalPlanner physicalPlanner;
  private static AbstractStorageManager sm;

  @BeforeClass
  public static void setUp() throws IOException {
    GlobalEngine engine = testingCluster.getMaster().getContext().getGlobalEngine();
    analyzer = engine.getSQLAnalyzer();
    planner = engine.getLogicalPlanner();
    optimizer = engine.getLogicalOptimizer();

    Path path = CommonTestingUtil.getTestDir("target/test-data/TestBlockExecutor");
    sm = StorageManagerFactory.getStorageManager(conf, path);

    physicalPlanner = new PhysicalPlannerImpl(conf, sm);
  }

  private static int i = 0;
  static Path outputPath;

  /**
   * Build a physical execution plan, which is a tree consisting of a number of physical executors.
   *
   * @param sql a SQL statement
   * @return Physical Execution Plan
   * @throws PlanningException
   * @throws IOException
   */
  public static PhysicalExec buildPhysicalPlan(String sql) throws PlanningException, IOException {
    Expr expr = analyzer.parse(sql);

    QueryContext context = LocalTajoTestingUtility.createDummyContext(conf);
    LogicalPlan plan = planner.createPlan(context, expr);
    optimizer.optimize(context, plan);

    LogicalNode [] founds = PlannerUtil.findAllNodes(plan.getRootBlock().getRoot(), NodeType.SCAN);

    List<FileFragment> mergedFragments = Lists.newArrayList();

    for (LogicalNode node : founds) {
      ScanNode scan = (ScanNode) node;
      TableDesc table = scan.getTableDesc();
      FileFragment[] frags = StorageManager.splitNG(conf, scan.getCanonicalName(), table.getMeta(), table.getPath(),
          Integer.MAX_VALUE);

      for (FileFragment f : frags) {
        mergedFragments.add(f);
      }
    }

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testdir_" + (i++));

    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newQueryUnitAttemptId(), mergedFragments.toArray(new FileFragment[mergedFragments.size()]), workDir);

    outputPath = new Path(workDir, "output");
    ctx.setOutputPath(outputPath);
    ctx.setEnforcer(new Enforcer());
    ctx.getQueryContext().setBool(SessionVars.CODEGEN, false);

    return physicalPlanner.createPlan(ctx, plan.getRootBlock().getRoot());
  }

  @Test
  public void testSeqScan() throws IOException, PlanningException {
    PhysicalExec exec = buildPhysicalPlan("select * from lineitem");

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(exec.getSchema(), 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    exec.init();

    int countForTuple = 0;
    int countForRowBlock = 0;
    while(exec.nextFetch(rowBlock)) {
      ZeroCopyTuple tuple = new ZeroCopyTuple();
      RowBlockReader reader =  rowBlock.getReader();
      while (reader.next(tuple)) {
        countForTuple++;
      }
      countForRowBlock += rowBlock.rows();
    }
    exec.close();
    rowBlock.release();

    assertEquals(5, countForTuple);
    assertEquals(5, countForRowBlock);
  }

  @Test
  public void testScanWithProjector() throws IOException, PlanningException {
    PhysicalExec exec = buildPhysicalPlan("select l_orderkey, l_partkey from lineitem");

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(exec.getSchema(), 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    exec.init();

    int countForTuple = 0;
    int countForRowBlock = 0;
    while(exec.nextFetch(rowBlock)) {
      ZeroCopyTuple tuple = new ZeroCopyTuple();
      RowBlockReader reader =  rowBlock.getReader();
      while (reader.next(tuple)) {
        countForTuple++;
      }
      countForRowBlock += rowBlock.rows();
    }
    exec.close();
    rowBlock.release();

    assertEquals(5, countForTuple);
    assertEquals(5, countForRowBlock);
  }

  @Test
  public void testStoreTableExec() throws IOException, PlanningException {
    PhysicalExec exec = buildPhysicalPlan("create table t1 using CSV as select * from lineitem");


    OffHeapRowBlock rowBlock = new OffHeapRowBlock(exec.getSchema(), 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    exec.init();

    int countForTuple = 0;
    int countForRowBlock = 0;
    while(exec.nextFetch(rowBlock)) {
      ZeroCopyTuple tuple = new ZeroCopyTuple();
      RowBlockReader reader =  rowBlock.getReader();
      while (reader.next(tuple)) {
        countForTuple++;
      }
      countForRowBlock += rowBlock.rows();
    }
    exec.close();

    assertEquals(5, countForTuple);
    assertEquals(5, countForRowBlock);

    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getFileScanner(meta, exec.getSchema(), outputPath);
    scanner.init();

    int readTupleCount = 0;
    while (scanner.nextFetch(rowBlock)) {
      readTupleCount += rowBlock.rows();
    }
    scanner.close();

    assertEquals(5, readTupleCount);

    rowBlock.release();
  }
}