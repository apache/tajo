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

/**
 * 
 */
package tajo.engine.planner.global;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.QueryId;
import tajo.QueryIdFactory;
import tajo.TajoTestingCluster;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.eval.TestEvalTree.TestSum;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.PlanningContext;
import tajo.engine.planner.logical.*;
import tajo.master.GlobalPlanner;
import tajo.master.SubQuery;
import tajo.storage.*;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGlobalQueryOptimizer {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static GlobalPlanner planner;
  private static Schema schema;
  private static QueryAnalyzer analyzer;
  private static LogicalPlanner logicalPlanner;
  private static QueryId queryId;
  private static GlobalOptimizer optimizer;

  @BeforeClass
  public static void setup() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    int i, j;

    schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("salary", DataType.INT);

    TableMeta meta;

    conf = new TajoConf(util.getConfiguration());
    catalog = util.getMiniCatalogCluster().getCatalog();
    StorageManager sm = new StorageManager(util.getConfiguration());
    FunctionDesc funcDesc = new FunctionDesc("sumtest", TestSum.class, FunctionType.GENERAL,
        new DataType [] {DataType.INT},
        new DataType [] {DataType.INT});
    catalog.registerFunction(funcDesc);
    FileSystem fs = sm.getFileSystem();

    AsyncDispatcher dispatcher = new AsyncDispatcher();

    planner = new GlobalPlanner(conf, catalog, new StorageManager(conf),
        dispatcher.getEventHandler());
    analyzer = new QueryAnalyzer(catalog);
    logicalPlanner = new LogicalPlanner(catalog);

    int tbNum = 2;
    int tupleNum;
    Appender appender;
    Tuple t = new VTuple(4);
    t.put(new Datum[] {
        DatumFactory.createInt(1), DatumFactory.createInt(32),
        DatumFactory.createString("h"), DatumFactory.createInt(10)});

    for (i = 0; i < tbNum; i++) {
      meta = TCatUtil.newTableMeta((Schema)schema.clone(), StoreType.CSV);
      meta.putOption(CSVFile.DELIMITER, ",");

      Path dataRoot = sm.getBaseDir();
      Path tablePath = StorageUtil.concatPath(dataRoot, "table"+i, "file.csv");
      if (fs.exists(tablePath.getParent())) {
        fs.delete(tablePath.getParent(), true);
      }
      fs.mkdirs(tablePath.getParent());
      appender = StorageManager.getAppender(conf, meta, tablePath);
      tupleNum = 100;
      for (j = 0; j < tupleNum; j++) {
        appender.addTuple(t);
      }
      appender.close();

      TableDesc desc = TCatUtil.newTableDesc("table" + i, (TableMeta)meta.clone(), sm.getTablePath("table"+i));
      catalog.addTable(desc);
    }

    QueryIdFactory.reset();
    queryId = QueryIdFactory.newQueryId();
    optimizer = new GlobalOptimizer();
  }
  
  @AfterClass
  public static void terminate() throws IOException {
    util.shutdownCatalogCluster();
  }

  @Test
  public void testReduceLogicalQueryUnitSteps() throws IOException {
    PlanningContext context = analyzer.parse(
        "select table0.age,table0.salary,table1.salary from table0,table1 where table0.salary = table1.salary order by table0.age");
    LogicalNode plan = logicalPlanner.createPlan(context);
    plan = LogicalOptimizer.optimize(context, plan);

    MasterPlan globalPlan = planner.build(queryId,
        (LogicalRootNode) plan);
    globalPlan = optimizer.optimize(globalPlan);
    
    SubQuery unit = globalPlan.getRoot();
    StoreTableNode store = unit.getStoreTableNode();
    assertEquals(ExprType.PROJECTION, store.getSubNode().getType());
    ProjectionNode proj = (ProjectionNode) store.getSubNode();
    assertEquals(ExprType.SORT, proj.getSubNode().getType());
    SortNode sort = (SortNode) proj.getSubNode();
    assertEquals(ExprType.SCAN, sort.getSubNode().getType());
    ScanNode scan = (ScanNode) sort.getSubNode();
    
    assertTrue(unit.hasChildQuery());
    unit = unit.getChildQuery(scan);
    store = unit.getStoreTableNode();
    assertEquals(ExprType.SORT, store.getSubNode().getType());
    sort = (SortNode) store.getSubNode();
    assertEquals(ExprType.JOIN, sort.getSubNode().getType());
    
    assertTrue(unit.hasChildQuery());
    for (ScanNode prevscan : unit.getScanNodes()) {
      SubQuery prev = unit.getChildQuery(prevscan);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.SCAN, store.getSubNode().getType());
    }
  }
}
