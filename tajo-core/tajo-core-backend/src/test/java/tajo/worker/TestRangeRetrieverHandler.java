/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.worker;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.QueryIdFactory;
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
import tajo.engine.planner.physical.ExternalSortExec;
import tajo.engine.planner.physical.IndexedStoreExec;
import tajo.engine.planner.physical.PhysicalExec;
import tajo.engine.planner.physical.ProjectionExec;
import tajo.storage.*;
import tajo.storage.index.bst.BSTIndex;
import tajo.util.CommonTestingUtil;
import tajo.util.TUtil;
import tajo.worker.dataserver.retriever.FileChunk;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRangeRetrieverHandler {
  private TajoTestingCluster util;
  private TajoConf conf;
  private CatalogService catalog;
  private QueryAnalyzer analyzer;
  private LogicalPlanner planner;
  private StorageManager sm;
  private Schema schema;
  private static int TEST_TUPLE = 10000;
  private FileSystem fs;
  private Path testDir;

  @Before
  public void setUp() throws Exception {
    QueryIdFactory.reset();
    util = new TajoTestingCluster();
    conf = util.getConfiguration();
    testDir = CommonTestingUtil.getTestDir("target/test-data/TestRangeRetrieverHandler");
    fs = testDir.getFileSystem(conf);
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    sm = StorageManager.get(conf, testDir);

    analyzer = new QueryAnalyzer(catalog);
    planner = new LogicalPlanner(catalog);

    schema = new Schema();
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("age", DataType.INT);
  }

  @After
  public void tearDown() {
    util.shutdownCatalogCluster();
  }

  public String [] SORT_QUERY = {
      "select empId, age from employee order by empId, age",
      "select empId, age from employee order by empId desc, age desc"
  };

  @Test
  public void testGet() throws Exception {
    Tuple firstTuple = null;
    Tuple lastTuple;

    TableMeta employeeMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    Path tableDir = StorageUtil.concatPath(testDir, "testGet", "table.csv");
    fs.mkdirs(tableDir.getParent());
    Appender appender = sm.getAppender(conf, employeeMeta, tableDir);

    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < TEST_TUPLE; i++) {
      tuple.put(
          new Datum[] {
              DatumFactory.createInt(i),
              DatumFactory.createInt(i+5)
          });
      appender.addTuple(tuple);

      if (firstTuple == null) {
        firstTuple = new VTuple(tuple);
      }
    }
    lastTuple = new VTuple(tuple);
    appender.flush();
    appender.close();

    TableDesc employee = new TableDescImpl("employee", employeeMeta, tableDir);
    catalog.addTable(employee);

    Fragment[] frags = StorageManager.splitNG(conf, "employee", employeeMeta, tableDir, Integer.MAX_VALUE);

    TaskAttemptContext
        ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] {frags[0]}, testDir);
    PlanningContext context = analyzer.parse(SORT_QUERY[0]);
    LogicalNode plan = planner.createPlan(context);
    plan = LogicalOptimizer.optimize(context, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;
    ExternalSortExec sort = (ExternalSortExec) proj.getChild();

    SortSpec[] sortSpecs = sort.getPlan().getSortKeys();
    IndexedStoreExec idxStoreExec = new IndexedStoreExec(ctx, sm, sort, sort.getSchema(),
        sort.getSchema(), sortSpecs);

    exec = idxStoreExec;
    exec.init();
    exec.next();
    exec.close();

    Schema keySchema = PlannerUtil.sortSpecsToSchema(sortSpecs);
    TupleComparator comp = new TupleComparator(keySchema, sortSpecs);
    BSTIndex bst = new BSTIndex(conf);
    BSTIndex.BSTIndexReader reader = bst.getIndexReader(
        new Path(testDir, "output/index"), keySchema, comp);
    reader.open();
    SingleFileScanner scanner = (SingleFileScanner)
        sm.getScanner(conf, employeeMeta, StorageUtil.concatPath(testDir, "output", "output"));

    int cnt = 0;
    while(scanner.next() != null) {
      cnt++;
    }
    scanner.reset();

    assertEquals(TEST_TUPLE ,cnt);

    Tuple keytuple = new VTuple(2);
    for(int i = 1 ; i < TEST_TUPLE ; i ++) {
      keytuple.put(0, DatumFactory.createInt(i));
      keytuple.put(1, DatumFactory.createInt(i + 5));
      long offsets = reader.find(keytuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]" , i == tuple.get(0).asInt());
      //assertTrue("[seek check " + (i) + " ]" , ("name_" + i).equals(tuple.get(1).asChars()));
    }

    TupleRange totalRange = new TupleRange(keySchema, firstTuple, lastTuple);
    UniformRangePartition partitioner = new UniformRangePartition(keySchema, totalRange, true);
    TupleRange [] partitions = partitioner.partition(7);

    // The below is for testing RangeRetrieverHandler.
    RangeRetrieverHandler handler = new RangeRetrieverHandler(
        new File((new Path(testDir, "output")).toUri()), keySchema, comp);

    List<Long []> offsets = new ArrayList<>();

    for (int i = 0; i < partitions.length; i++) {
      FileChunk chunk = getFileChunk(handler, keySchema, partitions[i], i == (partitions.length - 1));
      offsets.add(new Long[] {chunk.startOffset(), chunk.length()});
    }
    scanner.close();

    Long[] previous = null;
    for (Long [] offset : offsets) {
      if (offset[0] == 0 && previous == null) {
        previous = offset;
        continue;
      }
      assertTrue(previous[0] + previous[1] == offset[0]);
      previous = offset;
    }
    long fileLength = new File((new Path(testDir, "index").toUri())).length();
    assertTrue(previous[0] + previous[1] == fileLength);
  }

  @Test
  public void testGetFromDescendingOrder() throws Exception {
    Tuple firstTuple = null;
    Tuple lastTuple;

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    Path tablePath = StorageUtil.concatPath(testDir, "testGetFromDescendingOrder", "table.csv");
    fs.mkdirs(tablePath.getParent());
    Appender appender = sm.getAppender(conf, meta, tablePath);
    Tuple tuple = new VTuple(meta.getSchema().getColumnNum());
    for (int i = (TEST_TUPLE - 1); i >= 0 ; i--) {
      tuple.put(
          new Datum[] {
              DatumFactory.createInt(i),
              DatumFactory.createInt(i+5)
          });
      appender.addTuple(tuple);

      if (firstTuple == null) {
        firstTuple = new VTuple(tuple);
      }
    }
    lastTuple = new VTuple(tuple);
    appender.flush();
    appender.close();

    TableDesc employee = new TableDescImpl("employee", meta, tablePath);
    catalog.addTable(employee);

    Fragment[] frags = sm.splitNG(conf, "employee", meta, tablePath, Integer.MAX_VALUE);

    TaskAttemptContext
        ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] {frags[0]}, testDir);
    PlanningContext context = analyzer.parse(SORT_QUERY[1]);
    LogicalNode plan = planner.createPlan(context);
    plan = LogicalOptimizer.optimize(context, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;
    ExternalSortExec sort = (ExternalSortExec) proj.getChild();
    SortSpec[] sortSpecs = sort.getPlan().getSortKeys();
    IndexedStoreExec idxStoreExec = new IndexedStoreExec(ctx, sm, sort,
        sort.getSchema(), sort.getSchema(), sortSpecs);

    exec = idxStoreExec;
    exec.init();
    exec.next();
    exec.close();

    Schema keySchema = PlannerUtil.sortSpecsToSchema(sortSpecs);
    TupleComparator comp = new TupleComparator(keySchema, sortSpecs);
    BSTIndex bst = new BSTIndex(conf);
    BSTIndex.BSTIndexReader reader = bst.getIndexReader(
        new Path(testDir, "output/index"), keySchema, comp);
    reader.open();
    SingleFileScanner scanner = (SingleFileScanner) StorageManager.getScanner(
        conf, meta, StorageUtil.concatPath(testDir, "output", "output"));

    int cnt = 0;
    while(scanner.next() != null) {
      cnt++;
    }
    scanner.reset();

    assertEquals(TEST_TUPLE ,cnt);

    Tuple keytuple = new VTuple(2);
    for(int i = (TEST_TUPLE - 1) ; i >= 0; i --) {
      keytuple.put(0, DatumFactory.createInt(i));
      keytuple.put(1, DatumFactory.createInt(i + 5));
      long offsets = reader.find(keytuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]" , i == tuple.get(0).asInt());
    }

    TupleRange totalRange = new TupleRange(keySchema, lastTuple, firstTuple);
    UniformRangePartition partitioner = new UniformRangePartition(keySchema, totalRange, true);
    TupleRange [] partitions = partitioner.partition(25);

    File dataFile = new File((new Path(testDir, "output")).toUri());

    // The below is for testing RangeRetrieverHandler.
    RangeRetrieverHandler handler = new RangeRetrieverHandler(
        dataFile, keySchema, comp);

    List<Long []> offsets = new ArrayList<>();

    for (int i = 0; i < partitions.length; i++) {
      FileChunk chunk = getFileChunk(handler, keySchema, partitions[i], i == 0);
      offsets.add(new Long[] {chunk.startOffset(), chunk.length()});
    }
    scanner.close();

    long fileLength = new File(dataFile, "data/data").length();
    Long[] previous = null;
    for (Long [] offset : offsets) {
      if (previous == null) {
        assertTrue(offset[0] + offset[1] == fileLength);
        previous = offset;
        continue;
      }

      assertTrue(offset[0] + offset[1] == previous[0]);
      previous = offset;
    }
  }

  private FileChunk getFileChunk(RangeRetrieverHandler handler, Schema keySchema,
                                 TupleRange range, boolean last) throws IOException {
    Map<String,List<String>> kvs = Maps.newHashMap();
    kvs.put("start", Lists.newArrayList(
        new String(Base64.encodeBase64(
            RowStoreUtil.RowStoreEncoder.toBytes(keySchema, range.getStart()),
            false))));
    kvs.put("end", Lists.newArrayList(
        new String(Base64.encodeBase64(
            RowStoreUtil.RowStoreEncoder.toBytes(keySchema, range.getEnd()), false))));

    if (last) {
      kvs.put("final", Lists.newArrayList("true"));
    }
    return handler.get(kvs);
  }
}
