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

/**
 * 
 */
package org.apache.tajo.engine.planner.global;

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
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.master.ExecutionBlock;
import org.apache.tajo.master.GlobalPlanner;
import org.apache.tajo.storage.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGlobalQueryOptimizer {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static GlobalPlanner planner;
  private static Schema schema;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner logicalPlanner;
  private static QueryId queryId;
  private static GlobalOptimizer optimizer;

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
    StorageManager sm = new StorageManager(util.getConfiguration());
    FunctionDesc funcDesc = new FunctionDesc("sumtest", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4));
    catalog.registerFunction(funcDesc);
    FileSystem fs = sm.getFileSystem();

    AsyncDispatcher dispatcher = new AsyncDispatcher();

    planner = new GlobalPlanner(conf, catalog, new StorageManager(conf),
        dispatcher.getEventHandler());
    analyzer = new SQLAnalyzer();
    logicalPlanner = new LogicalPlanner(catalog);

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

      TableDesc desc = CatalogUtil
          .newTableDesc("table" + i, (TableMeta) meta.clone(), sm.getTablePath("table" + i));
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
  public void testReduceLogicalQueryUnitSteps() throws IOException, PlanningException {
    Expr expr = analyzer.parse(
        "select table0.age,table0.salary,table1.salary from table0,table1 where table0.salary = table1.salary order by table0.age");
    LogicalPlan plan = logicalPlanner.createPlan(expr);
    LogicalNode rootNode = LogicalOptimizer.optimize(plan);

    MasterPlan globalPlan = planner.build(queryId, (LogicalRootNode) rootNode);
    globalPlan = optimizer.optimize(globalPlan);
    
    ExecutionBlock unit = globalPlan.getRoot();
    StoreTableNode store = unit.getStoreTableNode();
    assertEquals(ExprType.PROJECTION, store.getSubNode().getType());
    ProjectionNode proj = (ProjectionNode) store.getSubNode();
    assertEquals(ExprType.SORT, proj.getSubNode().getType());
    SortNode sort = (SortNode) proj.getSubNode();
    assertEquals(ExprType.SCAN, sort.getSubNode().getType());
    ScanNode scan = (ScanNode) sort.getSubNode();
    
    assertTrue(unit.hasChildBlock());
    unit = unit.getChildBlock(scan);
    store = unit.getStoreTableNode();
    assertEquals(ExprType.SORT, store.getSubNode().getType());
    sort = (SortNode) store.getSubNode();
    assertEquals(ExprType.JOIN, sort.getSubNode().getType());
    
    assertTrue(unit.hasChildBlock());
    for (ScanNode prevscan : unit.getScanNodes()) {
      ExecutionBlock prev = unit.getChildBlock(prevscan);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.SCAN, store.getSubNode().getType());
    }
  }
}
