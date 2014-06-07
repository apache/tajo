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

package org.apache.tajo.engine.planner.global;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalOptimizer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static junit.framework.Assert.assertNotNull;
import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBroadcastJoinPlan {
  private TajoConf conf;
  private final String TEST_PATH = "target/test-data/TestBroadcastJoinPlan";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private Path testDir;

  private TableDesc smallTable1;
  private TableDesc smallTable2;
  private TableDesc smallTable3;
  private TableDesc largeTable1;
  private TableDesc largeTable2;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    conf = util.getConfiguration();
    conf.setLongVar(TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD, 500 * 1024);
    conf.setBoolVar(TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO, true);

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog = util.startCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    util.getMiniCatalogCluster().getCatalogServer().reloadBuiltinFunctions(TajoMaster.initBuiltinFunctions());

    Schema smallTable1Schema = new Schema();
    smallTable1Schema.addColumn("small1_id", TajoDataTypes.Type.INT4);
    smallTable1Schema.addColumn("small1_contents", TajoDataTypes.Type.TEXT);
    smallTable1 = makeTestData("default.small1", smallTable1Schema, 10 * 1024);

    Schema smallTable2Schema = new Schema();
    smallTable2Schema.addColumn("small2_id", TajoDataTypes.Type.INT4);
    smallTable2Schema.addColumn("small2_contents", TajoDataTypes.Type.TEXT);
    smallTable2 = makeTestData("default.small2", smallTable2Schema, 10 * 1024);

    Schema smallTable3Schema = new Schema();
    smallTable3Schema.addColumn("small3_id", TajoDataTypes.Type.INT4);
    smallTable3Schema.addColumn("small3_contents", TajoDataTypes.Type.TEXT);
    smallTable3 = makeTestData("default.small3", smallTable3Schema, 10 * 1024);

    Schema largeTable1Schema = new Schema();
    largeTable1Schema.addColumn("large1_id", TajoDataTypes.Type.INT4);
    largeTable1Schema.addColumn("large1_contents", TajoDataTypes.Type.TEXT);
    largeTable1 = makeTestData("default.large1", largeTable1Schema, 1024 * 1024);  //1M

    Schema largeTable2Schema = new Schema();
    largeTable2Schema.addColumn("large2_id", TajoDataTypes.Type.INT4);
    largeTable2Schema.addColumn("large2_contents", TajoDataTypes.Type.TEXT);
    largeTable2 = makeTestData("default.large2", largeTable2Schema, 1024 * 1024);  //1M

    catalog.createTable(smallTable1);
    catalog.createTable(smallTable2);
    catalog.createTable(largeTable1);
    catalog.createTable(largeTable2);

    analyzer = new SQLAnalyzer();
  }

  private TableDesc makeTestData(String tableName, Schema schema, int dataSize) throws Exception {
    TableMeta tableMeta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
    Path dataPath = new Path(testDir, tableName + ".csv");

    String contentsData = "";
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < 10; j++) {
        contentsData += j;
      }
    }
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(tableMeta, schema,
        dataPath);
    appender.init();
    Tuple tuple = new VTuple(schema.size());
    int writtenSize = 0;
    int count = 0;
    while (true) {
      TextDatum textDatum = DatumFactory.createText(count + "_" + contentsData);
      tuple.put(new Datum[] {
          DatumFactory.createInt4(count), textDatum });
      appender.addTuple(tuple);

      writtenSize += textDatum.size();
      if (writtenSize >= dataSize) {
        break;
      }
    }

    appender.flush();
    appender.close();

    TableDesc tableDesc = CatalogUtil.newTableDesc(tableName, schema, tableMeta, dataPath);
    TableStats tableStats = new TableStats();
    FileSystem fs = dataPath.getFileSystem(conf);
    tableStats.setNumBytes(fs.getFileStatus(dataPath).getLen());

    tableDesc.setStats(tableStats);

    return tableDesc;
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  @Test
  public final void testBroadcastJoin() throws IOException, PlanningException {
    String query = "select count(*) from large1 " +
        "join small1 on large1_id = small1_id " +
        "join small2 on small1_id = small2_id";

    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalOptimizer optimizer = new LogicalOptimizer(conf);
    Expr expr =  analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext();
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(masterPlan);

    /*
    |-eb_1395714781593_0000_000007 (TERMINAL)
        |-eb_1395714781593_0000_000006 (ROOT)
            |-eb_1395714781593_0000_000005 (LEAF)
    */

    ExecutionBlock terminalEB = masterPlan.getRoot();
    assertEquals(1, masterPlan.getChildCount(terminalEB.getId()));

    ExecutionBlock rootEB = masterPlan.getChild(terminalEB.getId(), 0);
    assertEquals(1, masterPlan.getChildCount(rootEB.getId()));

    ExecutionBlock leafEB = masterPlan.getChild(rootEB.getId(), 0);
    assertNotNull(leafEB);

    assertEquals(0, masterPlan.getChildCount(leafEB.getId()));
    Collection<String> broadcastTables = leafEB.getBroadcastTables();
    assertEquals(2, broadcastTables.size());

    assertTrue(broadcastTables.contains("default.small1"));
    assertTrue(broadcastTables.contains("default.small2"));
    assertTrue(!broadcastTables.contains("default.large1"));

    LogicalNode leafNode = leafEB.getPlan();
    assertEquals(NodeType.GROUP_BY, leafNode.getType());

    LogicalNode joinNode = ((GroupbyNode)leafNode).getChild();
    assertEquals(NodeType.JOIN, joinNode.getType());

    LogicalNode leftNode = ((JoinNode)joinNode).getLeftChild();
    LogicalNode rightNode = ((JoinNode)joinNode).getRightChild();

    assertEquals(NodeType.JOIN, leftNode.getType());
    assertEquals(NodeType.SCAN, rightNode.getType());

    LogicalNode lastLeftNode = ((JoinNode)leftNode).getLeftChild();
    LogicalNode lastRightNode = ((JoinNode)leftNode).getRightChild();

    assertEquals(NodeType.SCAN, lastLeftNode.getType());
    assertEquals(NodeType.SCAN, lastRightNode.getType());
  }

  @Test
  public final void testNotBroadcastJoinTwoLargeTable() throws IOException, PlanningException {
    // This query is not broadcast join
    String query = "select count(*) from large1 " +
        "join large2 on large1_id = large2_id ";

    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalOptimizer optimizer = new LogicalOptimizer(conf);
    Expr expr =  analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext();
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(masterPlan);

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      Collection<String> broadcastTables = eb.getBroadcastTables();
      assertTrue(broadcastTables == null || broadcastTables.isEmpty());
    }
  }

  @Test
  public final void testTwoBroadcastJoin() throws IOException, PlanningException {
    String query = "select count(*) from large1 " +
        "join small1 on large1_id = small1_id " +
        "join large2 on large1_id = large2_id " +
        "join small2 on large2_id = small2_id";

    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalOptimizer optimizer = new LogicalOptimizer(conf);
    Expr expr =  analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext();
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(masterPlan);

    /*
    |-eb_1395736346625_0000_000009
      |-eb_1395736346625_0000_000008 (GROUP-BY)
         |-eb_1395736346625_0000_000007 (GROUP-BY, JOIN)
           |-eb_1395736346625_0000_000006 (LEAF, JOIN)
           |-eb_1395736346625_0000_000003 (LEAF, JOIN)
     */

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(1, broadcastTables.size());

        assertTrue(!broadcastTables.contains("default.large1"));
        assertTrue(broadcastTables.contains("default.small1"));
      } else if(index == 1) {
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(1, broadcastTables.size());
        assertTrue(!broadcastTables.contains("default.large2"));
        assertTrue(broadcastTables.contains("default.small2"));
      }
      index++;
    }

    assertEquals(5, index);
  }

  @Test
  public final void testNotBroadcastJoinSubquery() throws IOException, PlanningException {
    // This query is not broadcast join;
    String query = "select count(*) from large1 " +
        "join (select * from small1) a on large1_id = a.small1_id " +
        "join small2 on a.small1_id = small2_id";

    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalOptimizer optimizer = new LogicalOptimizer(conf);
    Expr expr =  analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext();
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(masterPlan);

    /*
    |-eb_1395749810370_0000_000007
       |-eb_1395749810370_0000_000006 (GROUP-BY)
          |-eb_1395749810370_0000_000005 (GROUP-BY, JOIN)
             |-eb_1395749810370_0000_000004 (LEAF, SCAN, large1)
             |-eb_1395749810370_0000_000003 (JOIN)
                |-eb_1395749810370_0000_000002 (LEAF, SCAN, small2)
                |-eb_1395749810370_0000_000001 (LEAF, TABLE_SUBQUERY, small1)
     */

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      Collection<String> broadcastTables = eb.getBroadcastTables();
      assertTrue(broadcastTables == null || broadcastTables.isEmpty());
      index++;
    }

    assertEquals(7, index);
  }

  @Test
  public final void testBroadcastJoinSubquery() throws IOException, PlanningException {
    String query = "select count(*) from large1 " +
        "join small2 on large1_id = small2_id " +
        "join (select * from small1) a on large1_id = a.small1_id";

    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalOptimizer optimizer = new LogicalOptimizer(conf);
    Expr expr =  analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext();
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(masterPlan);

    /*
    |-eb_1395794091662_0000_000007
       |-eb_1395794091662_0000_000006
          |-eb_1395794091662_0000_000005 (JOIN)
             |-eb_1395794091662_0000_000004 (LEAF, SUBQUERY)
             |-eb_1395794091662_0000_000003 (LEAF, JOIN)
     */

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        //LEAF, JOIN
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(1, broadcastTables.size());

        assertTrue(!broadcastTables.contains("default.large1"));
        assertTrue(broadcastTables.contains("default.small2"));
      } else if(index == 1) {
        //LEAF, SUBQUERY
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertTrue(broadcastTables == null || broadcastTables.isEmpty());
      } else if(index == 2) {
        //JOIN
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertTrue(broadcastTables == null || broadcastTables.isEmpty());
      }
      index++;
    }

    assertEquals(5, index);
  }
}
