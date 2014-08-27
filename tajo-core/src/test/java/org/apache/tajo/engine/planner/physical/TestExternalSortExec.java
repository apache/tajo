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
import org.apache.tajo.storage.directmem.RowOrientedRowBlock;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.raw.TestDirectRawFile;
import org.apache.tajo.storage.rawfile.DirectRawFileScanner;
import org.apache.tajo.storage.rawfile.DirectRawFileWriter;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestExternalSortExec {
  private TajoConf conf;
  private TajoTestingCluster util;
  private final String TEST_PATH = "target/test-data/TestExternalSortExec";
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private AbstractStorageManager sm;
  private Path testDir;

  //private final int numTuple = 5;
  private final int numTuple = 10;
  private Random rnd = new Random(System.currentTimeMillis());

  private TableDesc employee;

  @Before
  public void setUp() throws Exception {
    this.conf = new TajoConf();
    util = new TajoTestingCluster();
    catalog = util.startCatalogCluster().getCatalog();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    conf.setVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR, testDir.toString());
    sm = StorageManagerFactory.getStorageManager(conf, testDir);

    Schema schema = new Schema();
    schema.addColumn("managerid", Type.INT4);
    schema.addColumn("empid", Type.INT4);
    schema.addColumn("deptname", Type.TEXT);

    TableMeta employeeMeta = CatalogUtil.newTableMeta(StoreType.DIRECTRAW);
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(employeeMeta, schema, employeePath);
    appender.enableStats();
    appender.init();

    long startTime = System.currentTimeMillis();

    Tuple tuple = new VTuple(schema.size());
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

    long endTime = System.currentTimeMillis();

    System.out.println(appender.getStats().getNumRows() + " rows (" + appender.getStats().getNumBytes() + " bytes)");
    System.out.println("writing takes " + (endTime - startTime) + " msec");

    employee = new TableDesc("default.employee", schema, employeeMeta, employeePath);
    catalog.createTable(employee);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
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
  public final void testNext() throws IOException, PlanningException {
    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    //queryContext.setInt(SessionVars.EXTSORT_BUFFER_SIZE, 1);

    FileFragment[] frags = StorageManager.splitNG(conf, "default.employee", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    Path workDir = new Path(testDir, TestExternalSortExec.class.getName());
    TaskAttemptContext ctx = new TaskAttemptContext(new QueryContext(conf),
        LocalTajoTestingUtility.newQueryUnitAttemptId(), new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(queryContext, expr);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    
    ProjectionExec proj = (ProjectionExec) exec;

    // TODO - should be planed with user's optimization hint
    if (!(proj.getChild() instanceof ExternalSortExec)) {
      UnaryPhysicalExec sortExec = proj.getChild();
      SeqScanExec scan = sortExec.getChild();

      ExternalSortExec extSort = new ExternalSortExec(ctx, sm,
          ((MemSortExec)sortExec).getPlan(), scan);
      proj.setChild(extSort);
    }

    Tuple tuple;
    Tuple preVal = null;
    Tuple curVal;
    int cnt = 0;
    exec.init();
    long start = System.currentTimeMillis();
    TupleComparator comparator = new TupleComparator(proj.getSchema(),
        new SortSpec[]{
            new SortSpec(new Column("managerid", Type.INT4)),
            new SortSpec(new Column("empid", Type.INT4))
        });

    while ((tuple = exec.next()) != null) {
      curVal = tuple;
      if (preVal != null) {
        assertTrue("prev: " + preVal + ", but cur: " + curVal, comparator.compare(preVal, curVal) <= 0);
      }
      preVal = curVal;
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
    System.out.println("Sort and final write time: " + (end - start) + " msc");
  }

  @Test
  public void testPairWiseMerger() throws IOException {
    int leftRowNum = 1000;
    int rightRowNum = 1234;
    RowOrientedRowBlock rowBlock1 = TestDirectRawFile.createRowBlock(leftRowNum);
    RowOrientedRowBlock rowBlock2 = TestDirectRawFile.createRowBlock(rightRowNum);

    TupleComparator comparator = new TupleComparator(TestDirectRawFile.schema,
        new SortSpec[] {new SortSpec(new Column("col2", Type.INT4))});
    List<Tuple> tupleList1 = ExternalSortExec.sortTuples(rowBlock1, comparator);
    List<Tuple> tupleList2 = ExternalSortExec.sortTuples(rowBlock2, comparator);

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.DIRECTRAW);
    Path dir = CommonTestingUtil.getTestDir();
    Path file1 = new Path(dir, "file1.out");
    Path file2 = new Path(dir, "file2.out");

    DirectRawFileWriter writer1 = new DirectRawFileWriter(conf, TestDirectRawFile.schema, meta, file1);
    writer1.init();
    for (Tuple t:tupleList1) {
      writer1.addTuple(t);
    }
    writer1.close();

    DirectRawFileWriter writer2 = new DirectRawFileWriter(conf, TestDirectRawFile.schema, meta, file2);
    writer2.init();
    for (Tuple t:tupleList2) {
      writer2.addTuple(t);
    }
    writer2.close();



    DirectRawFileScanner scanner1 = new DirectRawFileScanner(conf, TestDirectRawFile.schema, meta, file1);
    DirectRawFileScanner scanner2 = new DirectRawFileScanner(conf, TestDirectRawFile.schema, meta, file2);

    PairWiseMerger merger = new PairWiseMerger(TestDirectRawFile.schema, scanner1, scanner2, comparator);
    merger.init();

    Tuple tuple;
    Tuple curVal;
    Tuple preVal = null;
    int cnt = 0;
    while((tuple = merger.next()) != null) {
      curVal = tuple;
      if (preVal != null) {
        assertTrue("prev: " + preVal + ", but cur: " + curVal, comparator.compare(preVal, curVal) <= 0);
      }
      preVal = curVal;
      cnt++;
    }

    merger.close();

    scanner1.close();
    scanner2.close();

    assertEquals(leftRowNum + rightRowNum, cnt);
  }
}
