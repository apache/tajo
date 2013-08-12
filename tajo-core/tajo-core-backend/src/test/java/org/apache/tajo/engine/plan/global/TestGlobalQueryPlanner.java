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

package org.apache.tajo.engine.plan.global;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.TestEvalTree.TestSum;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalOptimizer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.master.ExecutionBlock;
import org.apache.tajo.master.ExecutionBlock.PartitionType;
import org.apache.tajo.master.GlobalPlanner;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.storage.*;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.*;

public class TestGlobalQueryPlanner {

  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static GlobalPlanner planner;
  private static Schema schema;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner logicalPlanner;
  private static LogicalOptimizer optimizer;
  private static QueryId queryId;
  private static StorageManager sm;

  @BeforeClass
  public static void setup() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();

    int i, j;

    schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("salary", Type.INT4);

    TableMeta meta;

    conf = new TajoConf(util.getConfiguration());
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.registerFunction(funcDesc);
    }

    sm = new StorageManager(util.getConfiguration());
    FunctionDesc funcDesc = new FunctionDesc("sumtest", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4));
    catalog.registerFunction(funcDesc);
    FileSystem fs = sm.getFileSystem();

    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();

    planner = new GlobalPlanner(conf, catalog, new StorageManager(conf),
        dispatcher.getEventHandler());
    analyzer = new SQLAnalyzer();
    logicalPlanner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer();

    int tbNum = 2;
    int tupleNum;
    Appender appender;
    Tuple t = new VTuple(4);
    t.put(new Datum[] {
        DatumFactory.createInt4(1), DatumFactory.createInt4(32),
        DatumFactory.createText("h"), DatumFactory.createInt4(10)});

    for (i = 0; i < tbNum; i++) {
      meta = CatalogUtil.newTableMeta((Schema) schema.clone(), StoreType.CSV);
      meta.putOption(CSVFile.DELIMITER, ",");

      Path dataRoot = sm.getBaseDir();
      Path tablePath = StorageUtil.concatPath(dataRoot, "table"+i, "file.csv");
      if (fs.exists(tablePath.getParent())) {
        fs.delete(tablePath.getParent(), true);
      }
      fs.mkdirs(tablePath.getParent());
      appender = StorageManager.getAppender(conf, meta, tablePath);
      appender.init();
      tupleNum = 100;
      for (j = 0; j < tupleNum; j++) {
        appender.addTuple(t);
      }
      appender.close();

      TableDesc desc = CatalogUtil.newTableDesc("table" + i, (TableMeta) meta.clone(), tablePath);
      catalog.addTable(desc);
    }

    QueryIdFactory.reset();
    queryId = QueryIdFactory.newQueryId();
    dispatcher.stop();
  }

  @AfterClass
  public static void terminate() throws IOException {
    util.shutdownCatalogCluster();
  }
  
  @Test
  public void testScan() throws IOException, PlanningException {
    Expr context = analyzer.parse(
        "select age, sumtest(salary) from table0");

    LogicalPlan plan = logicalPlanner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);


    MasterPlan globalPlan = planner.build(queryId,
        (LogicalRootNode) rootNode);

    ExecutionBlock unit = globalPlan.getRoot();
    assertFalse(unit.hasChildBlock());
    assertEquals(PartitionType.LIST, unit.getPartitionType());

    LogicalNode plan2 = unit.getPlan();
    assertEquals(ExprType.STORE, plan2.getType());
    assertEquals(ExprType.SCAN, ((StoreTableNode)plan2).getSubNode().getType());
  }

  @Test
  public void testGroupby() throws IOException, KeeperException,
      InterruptedException, PlanningException {
    Expr context = analyzer.parse(
        "create table store1 as select age, sumtest(salary) from table0 group by age");
    LogicalPlan plan = logicalPlanner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);

    MasterPlan globalPlan = planner.build(queryId, (LogicalRootNode) rootNode);

    ExecutionBlock next, prev;
    
    next = globalPlan.getRoot();
    assertTrue(next.hasChildBlock());
    assertEquals(PartitionType.LIST, next.getPartitionType());
    for (ScanNode scan : next.getScanNodes()) {
      assertTrue(scan.isLocal());
    }
    assertFalse(next.getStoreTableNode().isLocal());
    Iterator<ExecutionBlock> it= next.getChildBlocks().iterator();
    
    prev = it.next();
    assertFalse(prev.hasChildBlock());
    assertEquals(PartitionType.HASH, prev.getPartitionType());
    assertTrue(prev.getStoreTableNode().isLocal());
    assertFalse(it.hasNext());
    
    ScanNode []scans = prev.getScanNodes();
    assertEquals(1, scans.length);
    assertEquals("table0", scans[0].getTableId());
    assertFalse(scans[0].isLocal());
    
    scans = next.getScanNodes();
    assertEquals(1, scans.length);
    StoreTableNode store = prev.getStoreTableNode();
    assertEquals(store.getTableName(), scans[0].getTableId());
    assertEquals(store.getOutSchema(), scans[0].getInSchema());
  }
  
  @Test
  public void testSort() throws IOException, PlanningException {
    Expr context = analyzer.parse(
        "create table store1 as select age from table0 order by age");
    LogicalPlan plan = logicalPlanner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);

    MasterPlan globalPlan = planner.build(queryId, (LogicalRootNode) rootNode);

    ExecutionBlock next, prev;
    
    next = globalPlan.getRoot();
    assertEquals(ExprType.PROJECTION,
        next.getStoreTableNode().getSubNode().getType());
    assertTrue(next.hasChildBlock());
    assertEquals(PartitionType.LIST, next.getPartitionType());
    Iterator<ExecutionBlock> it= next.getChildBlocks().iterator();

    prev = it.next();
    assertEquals(ExprType.SORT,
        prev.getStoreTableNode().getSubNode().getType());
    assertTrue(prev.hasChildBlock());
    assertEquals(PartitionType.LIST, prev.getPartitionType());
    it= prev.getChildBlocks().iterator();
    next = prev;
    
    prev = it.next();
    assertFalse(prev.hasChildBlock());
    assertEquals(PartitionType.RANGE, prev.getPartitionType());
    assertFalse(it.hasNext());
    
    ScanNode []scans = prev.getScanNodes();
    assertEquals(1, scans.length);
    assertEquals("table0", scans[0].getTableId());
    
    scans = next.getScanNodes();
    assertEquals(1, scans.length);
    StoreTableNode store = prev.getStoreTableNode();
    assertEquals(store.getTableName(), scans[0].getTableId());
    assertEquals(store.getOutSchema(), scans[0].getInSchema());
  }
  
  @Test
  public void testJoin() throws IOException, PlanningException {
    Expr expr = analyzer.parse(
        "select table0.age,table0.salary,table1.salary from table0,table1 " +
            "where table0.salary = table1.salary order by table0.age");
    LogicalPlan plan = logicalPlanner.createPlan(expr);
    LogicalNode rootNode = optimizer.optimize(plan);


    MasterPlan globalPlan = planner.build(queryId, (LogicalRootNode) rootNode);

    ExecutionBlock next, prev;
    
    // the second phase of the sort
    next = globalPlan.getRoot();
    assertTrue(next.hasChildBlock());
    assertEquals(PartitionType.LIST, next.getPartitionType());
    assertEquals(ExprType.PROJECTION, next.getStoreTableNode().getSubNode().getType());
    ScanNode []scans = next.getScanNodes();
    assertEquals(1, scans.length);
    Iterator<ExecutionBlock> it= next.getChildBlocks().iterator();

    prev = it.next();
    assertEquals(ExprType.SORT, prev.getStoreTableNode().getSubNode().getType());
    assertEquals(PartitionType.LIST, prev.getPartitionType());
    scans = prev.getScanNodes();
    assertEquals(1, scans.length);
    it= prev.getChildBlocks().iterator();
    
    // the first phase of the sort
    prev = it.next();
    assertEquals(ExprType.SORT, prev.getStoreTableNode().getSubNode().getType());
    assertEquals(scans[0].getInSchema(), prev.getOutputSchema());
    assertTrue(prev.hasChildBlock());
    assertEquals(PartitionType.RANGE, prev.getPartitionType());
    assertFalse(it.hasNext());
    scans = prev.getScanNodes();
    assertEquals(1, scans.length);
    next = prev;
    it= next.getChildBlocks().iterator();
    
    // the second phase of the join
    prev = it.next();
    assertEquals(ExprType.JOIN, prev.getStoreTableNode().getSubNode().getType());
    assertEquals(scans[0].getInSchema(), prev.getOutputSchema());
    assertTrue(prev.hasChildBlock());
    assertEquals(PartitionType.LIST, prev.getPartitionType());
    assertFalse(it.hasNext());
    scans = prev.getScanNodes();
    assertEquals(2, scans.length);
    next = prev;
    it= next.getChildBlocks().iterator();
    
    // the first phase of the join
    prev = it.next();
    assertEquals(ExprType.SCAN, prev.getStoreTableNode().getSubNode().getType());
    assertFalse(prev.hasChildBlock());
    assertEquals(PartitionType.HASH, prev.getPartitionType());
    assertEquals(1, prev.getScanNodes().length);
    
    prev = it.next();
    assertEquals(ExprType.SCAN, prev.getStoreTableNode().getSubNode().getType());
    assertFalse(prev.hasChildBlock());
    assertEquals(PartitionType.HASH, prev.getPartitionType());
    assertEquals(1, prev.getScanNodes().length);
    assertFalse(it.hasNext());
  }
  
  @Test
  public void testSelectAfterJoin() throws IOException, PlanningException {
    String query = "select table0.name, table1.salary from table0,table1 where table0.name = table1.name and table1.salary > 10";
    Expr context = analyzer.parse(query);
    LogicalPlan plan = logicalPlanner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);
    
    MasterPlan globalPlan = planner.build(queryId, (LogicalRootNode) rootNode);

    ExecutionBlock unit = globalPlan.getRoot();
    StoreTableNode store = unit.getStoreTableNode();
    assertEquals(ExprType.JOIN, store.getSubNode().getType());
    assertTrue(unit.hasChildBlock());
    ScanNode [] scans = unit.getScanNodes();
    assertEquals(2, scans.length);
    ExecutionBlock prev;
    for (ScanNode scan : scans) {
      prev = unit.getChildBlock(scan);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.SCAN, store.getSubNode().getType());
    }
  }
  
  //@Test
  public void testCubeby() throws IOException, PlanningException {
    Expr expr = analyzer.parse(
        "select age, sum(salary) from table0 group by cube (age, id)");
    LogicalPlan plan = logicalPlanner.createPlan(expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    MasterPlan globalPlan = planner.build(queryId, (LogicalRootNode) rootNode);

    ExecutionBlock unit = globalPlan.getRoot();
    StoreTableNode store = unit.getStoreTableNode();
    assertEquals(ExprType.PROJECTION, store.getSubNode().getType());

    ScanNode[] scans = unit.getScanNodes();
    assertEquals(1, scans.length);

    unit = unit.getChildBlock(scans[0]);
    store = unit.getStoreTableNode();
    assertEquals(ExprType.UNION, store.getSubNode().getType());
    UnionNode union = (UnionNode) store.getSubNode();
    assertEquals(ExprType.SCAN, union.getOuterNode().getType());
    assertEquals(ExprType.UNION, union.getInnerNode().getType());
    union = (UnionNode) union.getInnerNode();
    assertEquals(ExprType.SCAN, union.getOuterNode().getType());
    assertEquals(ExprType.UNION, union.getInnerNode().getType());
    union = (UnionNode) union.getInnerNode();
    assertEquals(ExprType.SCAN, union.getOuterNode().getType());
    assertEquals(ExprType.SCAN, union.getInnerNode().getType());
    assertTrue(unit.hasChildBlock());
    
    String tableId = "";
    for (ScanNode scan : unit.getScanNodes()) {
      ExecutionBlock prev = unit.getChildBlock(scan);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.GROUP_BY, store.getSubNode().getType());
      GroupbyNode groupby = (GroupbyNode) store.getSubNode();
      assertEquals(ExprType.SCAN, groupby.getSubNode().getType());
      if (tableId.equals("")) {
        tableId = store.getTableName();
      } else {
        assertEquals(tableId, store.getTableName());
      }
      assertEquals(1, prev.getScanNodes().length);
      prev = prev.getChildBlock(prev.getScanNodes()[0]);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.GROUP_BY, store.getSubNode().getType());
      groupby = (GroupbyNode) store.getSubNode();
      assertEquals(ExprType.SCAN, groupby.getSubNode().getType());
    }
  }
}
