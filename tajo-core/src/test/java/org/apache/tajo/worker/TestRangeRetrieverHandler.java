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

package org.apache.tajo.worker;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.tajo.engine.planner.physical.ExternalSortExec;
import org.apache.tajo.engine.planner.physical.PhysicalExec;
import org.apache.tajo.engine.planner.physical.ProjectionExec;
import org.apache.tajo.engine.planner.physical.RangeShuffleFileWriteExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.worker.dataserver.retriever.FileChunk;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRangeRetrieverHandler {
  private TajoTestingCluster util;
  private TajoConf conf;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private AbstractStorageManager sm;
  private Schema schema;
  private static int TEST_TUPLE = 10000;
  private FileSystem fs;
  private Path testDir;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    conf = util.getConfiguration();
    testDir = CommonTestingUtil.getTestDir();
    fs = testDir.getFileSystem(conf);
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    sm = StorageManagerFactory.getStorageManager(conf, testDir);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);

    schema = new Schema();
    schema.addColumn("empid", Type.INT4);
    schema.addColumn("age", Type.INT4);
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

    TableMeta employeeMeta = CatalogUtil.newTableMeta(StoreType.CSV);

    Path tableDir = StorageUtil.concatPath(testDir, "testGet", "table.csv");
    fs.mkdirs(tableDir.getParent());
    Appender appender = sm.getAppender(employeeMeta, schema, tableDir);
    appender.init();

    Tuple tuple = new VTuple(schema.size());
    for (int i = 0; i < TEST_TUPLE; i++) {
      tuple.put(
          new Datum[] {
              DatumFactory.createInt4(i),
              DatumFactory.createInt4(i + 5)
          });
      appender.addTuple(tuple);

      if (firstTuple == null) {
        firstTuple = new VTuple(tuple);
      }
    }
    lastTuple = new VTuple(tuple);
    appender.flush();
    appender.close();

    TableDesc employee = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, employeeMeta,
        tableDir);
    catalog.createTable(employee);

    FileFragment[] frags = StorageManager.splitNG(conf, "default.employee", employeeMeta, tableDir, Integer.MAX_VALUE);

    TaskAttemptContext ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(),
        new FileFragment[] {frags[0]}, testDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(SORT_QUERY[0]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    ExternalSortExec sort = null;
    if (exec instanceof ProjectionExec) {
      ProjectionExec projExec = (ProjectionExec) exec;
      sort = projExec.getChild();
    } else if (exec instanceof ExternalSortExec) {
      sort = (ExternalSortExec) exec;
    } else {
      assertTrue(false);
    }

    SortSpec[] sortSpecs = sort.getPlan().getSortKeys();
    RangeShuffleFileWriteExec idxStoreExec = new RangeShuffleFileWriteExec(ctx, sm, sort, sort.getSchema(),
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

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.RAW, new KeyValueSet());
    SeekableScanner scanner = StorageManagerFactory.getSeekableScanner(conf, meta, schema,
        StorageUtil.concatPath(testDir, "output", "output"));

    scanner.init();
    int cnt = 0;
    while(scanner.next() != null) {
      cnt++;
    }
    scanner.reset();

    assertEquals(TEST_TUPLE ,cnt);

    Tuple keytuple = new VTuple(2);
    for(int i = 1 ; i < TEST_TUPLE ; i ++) {
      keytuple.put(0, DatumFactory.createInt4(i));
      keytuple.put(1, DatumFactory.createInt4(i + 5));
      long offsets = reader.find(keytuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]" , i == tuple.get(0).asInt4());
      //assertTrue("[seek check " + (i) + " ]" , ("name_" + i).equals(tuple.get(1).asChars()));
    }

    TupleRange totalRange = new TupleRange(sortSpecs, firstTuple, lastTuple);
    UniformRangePartition partitioner = new UniformRangePartition(totalRange, sortSpecs, true);
    TupleRange [] partitions = partitioner.partition(7);

    // The below is for testing RangeRetrieverHandler.
    RangeRetrieverHandler handler = new RangeRetrieverHandler(
        new File((new Path(testDir, "output")).toUri()), keySchema, comp);

    List<Long []> offsets = new ArrayList<Long []>();

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

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
    Path tablePath = StorageUtil.concatPath(testDir, "testGetFromDescendingOrder", "table.csv");
    fs.mkdirs(tablePath.getParent());
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();
    Tuple tuple = new VTuple(schema.size());
    for (int i = (TEST_TUPLE - 1); i >= 0 ; i--) {
      tuple.put(
          new Datum[] {
              DatumFactory.createInt4(i),
              DatumFactory.createInt4(i + 5)
          });
      appender.addTuple(tuple);

      if (firstTuple == null) {
        firstTuple = new VTuple(tuple);
      }
    }
    lastTuple = new VTuple(tuple);
    appender.flush();
    appender.close();

    TableDesc employee = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, meta, tablePath);
    catalog.createTable(employee);

    FileFragment[] frags = sm.splitNG(conf, "default.employee", meta, tablePath, Integer.MAX_VALUE);

    TaskAttemptContext
        ctx = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(),
        new FileFragment[] {frags[0]}, testDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(SORT_QUERY[1]);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    ExternalSortExec sort = null;
    if (exec instanceof ProjectionExec) {
      ProjectionExec projExec = (ProjectionExec) exec;
      sort = projExec.getChild();
    } else if (exec instanceof ExternalSortExec) {
      sort = (ExternalSortExec) exec;
    } else {
      assertTrue(false);
    }

    SortSpec[] sortSpecs = sort.getPlan().getSortKeys();
    RangeShuffleFileWriteExec idxStoreExec = new RangeShuffleFileWriteExec(ctx, sm, sort,
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
    TableMeta outputMeta = CatalogUtil.newTableMeta(StoreType.RAW, new KeyValueSet());
    SeekableScanner scanner = StorageManagerFactory.getSeekableScanner(conf, outputMeta, schema,
        StorageUtil.concatPath(testDir, "output", "output"));
    scanner.init();
    int cnt = 0;
    while(scanner.next() != null) {
      cnt++;
    }
    scanner.reset();

    assertEquals(TEST_TUPLE ,cnt);

    Tuple keytuple = new VTuple(2);
    for(int i = (TEST_TUPLE - 1) ; i >= 0; i --) {
      keytuple.put(0, DatumFactory.createInt4(i));
      keytuple.put(1, DatumFactory.createInt4(i + 5));
      long offsets = reader.find(keytuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]" , i == tuple.get(0).asInt4());
    }

    TupleRange totalRange = new TupleRange(sortSpecs, firstTuple, lastTuple);
    UniformRangePartition partitioner = new UniformRangePartition(totalRange, sortSpecs, true);
    TupleRange [] partitions = partitioner.partition(25);

    File dataFile = new File((new Path(testDir, "output")).toUri());

    // The below is for testing RangeRetrieverHandler.
    RangeRetrieverHandler handler = new RangeRetrieverHandler(
        dataFile, keySchema, comp);

    List<Long []> offsets = new ArrayList<Long []>();

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
    RowStoreEncoder encoder = RowStoreUtil.createEncoder(keySchema);
    kvs.put("start", Lists.newArrayList(
        new String(Base64.encodeBase64(
            encoder.toBytes(range.getStart()),
            false))));
    kvs.put("end", Lists.newArrayList(
        new String(Base64.encodeBase64(
            encoder.toBytes(range.getEnd()), false))));

    if (last) {
      kvs.put("final", Lists.newArrayList("true"));
    }
    return handler.get(kvs);
  }
}
