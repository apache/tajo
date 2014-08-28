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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.directmem.RowOrientedRowBlock;
import org.apache.tajo.storage.directmem.TestRowOrientedRowBlock;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.raw.TestDirectRawFile;
import org.apache.tajo.storage.rawfile.DirectRawFileScanner;
import org.apache.tajo.storage.rawfile.DirectRawFileWriter;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.apache.tajo.storage.directmem.TestRowOrientedRowBlock.schema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestExternalSortExec {
  private static final Log LOG = LogFactory.getLog(TestExternalSortExec.class);

  private TajoConf conf;
  private TajoTestingCluster util;
  private final String TEST_PATH = "target/test-data/TestExternalSortExec";
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private AbstractStorageManager sm;
  private Path testDir;

  private final int numTuple = 3000000;
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

    RowOrientedRowBlock rowBlock = TestRowOrientedRowBlock.createRowBlock(numTuple);
    TableMeta employeeMeta = CatalogUtil.newTableMeta(StoreType.DIRECTRAW);

    Path outFile = new Path(TEST_PATH, "output.draw");

    long startTime = System.currentTimeMillis();
    Path employeePath = TestDirectRawFile.writeRowBlock(conf, employeeMeta, rowBlock, outFile);
    long endTime = System.currentTimeMillis();
    rowBlock.free();

    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(employeePath);
    LOG.info("============================================================");
    LOG.info("Written file size: " + FileUtil.humanReadableByteCount(status.getLen(), false) + " " +
        (endTime - startTime) + " msec");
    LOG.info("============================================================");

    employee = new TableDesc("default.employee", schema, employeeMeta, employeePath);
    catalog.createTable(employee);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @After
  public void tearDown() throws Exception {
    CommonTestingUtil.cleanupTestDir(TEST_PATH);;
    util.shutdownCatalogCluster();
  }

  String[] QUERIES = {
      "select col2, col3, col4 from employee order by col2, col3"
  };

  @Test
  public final void testNext() throws IOException, PlanningException {
    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    //queryContext.setInt(SessionVars.EXTSORT_BUFFER_SIZE, 38);

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

    Tuple tuple;
    Tuple preVal = null;
    Tuple curVal;
    int cnt = 0;
    exec.init();
    long start = System.currentTimeMillis();
    TupleComparator comparator = new TupleComparator(exec.getSchema(),
        new SortSpec[]{
            new SortSpec(new Column("col2", Type.INT4)),
            new SortSpec(new Column("col3", Type.INT8))
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

  public static DirectRawFileScanner createSortedScanner(TajoConf conf, TableMeta meta, int rowNum,
                                                         TupleComparator comparator)
      throws IOException {
    Path testDir = CommonTestingUtil.getTestDir();
    Path outFile = new Path(testDir, "file1.out");

    RowOrientedRowBlock rowBlock = TestRowOrientedRowBlock.createRowBlock(rowNum);

    List<Tuple> tupleList = ExternalSortExec.sortTuples(rowBlock, comparator);

    DirectRawFileWriter writer1 = new DirectRawFileWriter(conf, schema, meta, outFile);
    writer1.init();
    for (Tuple t:tupleList) {
      writer1.addTuple(t);
    }
    writer1.close();

    return new DirectRawFileScanner(conf, schema, meta, outFile);
  }
}
