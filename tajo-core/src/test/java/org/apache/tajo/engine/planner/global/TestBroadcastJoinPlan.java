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

import junit.framework.TestCase;
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
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
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
  private TableDesc largeTable3;

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

    Schema largeTable3Schema = new Schema();
    largeTable3Schema.addColumn("large3_id", TajoDataTypes.Type.INT4);
    largeTable3Schema.addColumn("large3_contents", TajoDataTypes.Type.TEXT);
    largeTable3 = makeTestData("default.large3", largeTable3Schema, 1024 * 1024);  //1M

    catalog.createTable(smallTable1);
    catalog.createTable(smallTable2);
    catalog.createTable(smallTable3);
    catalog.createTable(largeTable1);
    catalog.createTable(largeTable2);
    catalog.createTable(largeTable3);

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
    |-eb_1395714781593_0000_000005 (TERMINAL)
        |-eb_1395714781593_0000_000004 (ROOT, GROUP BY for counting)
            |-eb_1395714781593_0000_000003 (LEAF, broadcast join)
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
  public final void testBroadcastJoinAllSmallTables() throws IOException, PlanningException {
    String query = "select count(*) from small1 " +
        "join small2 on small1_id = small2_id " +
        "join small3 on small1_id = small3_id";

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
    |-eb_1402500846700_0000_000005
       |-eb_1402500846700_0000_000004
          |-eb_1402500846700_0000_000003 (LEAF, broadcast join small1, small2, small3)
    */

    ExecutionBlock terminalEB = masterPlan.getRoot();
    assertEquals(1, masterPlan.getChildCount(terminalEB.getId()));

    ExecutionBlock rootEB = masterPlan.getChild(terminalEB.getId(), 0);
    assertEquals(1, masterPlan.getChildCount(rootEB.getId()));

    ExecutionBlock leafEB = masterPlan.getChild(rootEB.getId(), 0);
    assertNotNull(leafEB);

    assertEquals(0, masterPlan.getChildCount(leafEB.getId()));
    Collection<String> broadcastTables = leafEB.getBroadcastTables();
    assertEquals(3, broadcastTables.size());

    assertTrue(broadcastTables.contains("default.small2"));
    assertTrue(broadcastTables.contains("default.small1"));
    assertTrue(broadcastTables.contains("default.small3"));

    LogicalNode leafNode = leafEB.getPlan();
    assertEquals(NodeType.GROUP_BY, leafNode.getType());

    LogicalNode joinNode = ((GroupbyNode)leafNode).getChild();
    assertEquals(NodeType.JOIN, joinNode.getType());

    LogicalNode leftNode = ((JoinNode)joinNode).getLeftChild();
    LogicalNode rightNode = ((JoinNode)joinNode).getRightChild();

    assertEquals(NodeType.JOIN, leftNode.getType());
    assertEquals(NodeType.SCAN, rightNode.getType());
    assertEquals("default.small3", ((ScanNode)rightNode).getCanonicalName());

    LogicalNode lastLeftNode = ((JoinNode)leftNode).getLeftChild();
    LogicalNode lastRightNode = ((JoinNode)leftNode).getRightChild();

    assertEquals(NodeType.SCAN, lastLeftNode.getType());
    assertEquals(NodeType.SCAN, lastRightNode.getType());
    assertEquals("default.small1", ((ScanNode)lastLeftNode).getCanonicalName());
    assertEquals("default.small2", ((ScanNode)lastRightNode).getCanonicalName());
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

  @Test
  public final void testLeftOuterJoinCase1() throws IOException, PlanningException {
    // small, small, small, large, large
    String query = "select count(*) from small1 " +
        "left outer join small2 on small1_id = small2_id " +
        "left outer join small3 on small1_id = small3_id " +
        "left outer join large1 on small1_id = large1_id " +
        "left outer join large2 on small1_id = large2_id ";

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

    // ((((default.small1 ⟕ default.small2) ⟕ default.small3) ⟕ default.large1) ⟕ default.large2)
    /*
    |-eb_1402495213549_0000_000007
       |-eb_1402495213549_0000_000006       (GROUP BY)
          |-eb_1402495213549_0000_000005    (JOIN)
             |-eb_1402495213549_0000_000004 (LEAF, large2)
             |-eb_1402495213549_0000_000003 (LEAF, broadcast JOIN small1, small2, small3, large1)
     */

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(3, broadcastTables.size());

        assertTrue(broadcastTables.contains("default.small1"));
        assertTrue(broadcastTables.contains("default.small2"));
        assertTrue(broadcastTables.contains("default.small3"));
      } else if(index == 1 || index == 2 || index == 3) {
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(0, broadcastTables.size());
      }
      index++;
    }

    assertEquals(5, index);
  }

  @Test
  public final void testLeftOuterJoinCase2() throws IOException, PlanningException {
    // large, large, small, small, small
    String query = "select count(*) from large1 " +
        "left outer join large2 on large1_id = large2_id " +
        "left outer join small1 on large1_id = small1_id " +
        "left outer join small2 on large1_id = small2_id " +
        "left outer join small3 on large1_id = small3_id ";

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

    // ((((default.large1 ⟕ default.large2) ⟕ default.small1) ⟕ default.small2) ⟕ default.small3)
    /*
    |-eb_1404132555037_0000_000005
       |-eb_1404132555037_0000_000004
          |-eb_1404132555037_0000_000003    (JOIN, broadcast small1, small2, small3)
             |-eb_1404132555037_0000_000002 (LEAF, Scan large2)
             |-eb_1404132555037_0000_000001 (LEAF, Scan large1)
     */

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.SCAN, node.getType());
        assertEquals("default.large1", ((ScanNode) node).getCanonicalName());

        assertEquals(0, eb.getBroadcastTables().size());
      } else if (index == 1) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.SCAN, node.getType());
        assertEquals("default.large2", ((ScanNode)node).getCanonicalName());

        assertEquals(0, eb.getBroadcastTables().size());
      } else if(index == 2) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.GROUP_BY, node.getType());

        JoinNode joinNode = ((GroupbyNode)node).getChild();
        JoinNode joinNode2 = joinNode.getLeftChild();
        ScanNode scanNode2 = joinNode.getRightChild();
        assertEquals("default.small3", scanNode2.getCanonicalName());

        JoinNode joinNode3 = joinNode2.getLeftChild();
        ScanNode scanNode3 = joinNode2.getRightChild();
        assertEquals("default.small2", scanNode3.getCanonicalName());

        JoinNode joinNode4 = joinNode3.getLeftChild();
        ScanNode scanNode4 = joinNode3.getRightChild();
        assertEquals("default.small1", scanNode4.getCanonicalName());

        ScanNode scanNode5 = joinNode4.getLeftChild();
        ScanNode scanNode6 = joinNode4.getRightChild();
        assertTrue(scanNode5.getCanonicalName().indexOf("0000_000001") > 0);
        assertTrue(scanNode6.getCanonicalName().indexOf("0000_000002") > 0);

        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(3, broadcastTables.size());

        assertTrue(broadcastTables.contains("default.small1"));
        assertTrue(broadcastTables.contains("default.small2"));
        assertTrue(broadcastTables.contains("default.small3"));
      }
      index++;
    }

    assertEquals(5, index);
  }

  @Test
  public final void testLeftOuterJoinCase3() throws IOException, PlanningException {
    // large1, large2, small1, large3, small2, small3
    String query = "select count(*) from large1 " +
        "left outer join large2 on large1_id = large2_id " +
        "left outer join small1 on large2_id = small1_id " +
        "left outer join large3 on large1_id = large3_id " +
        "left outer join small2 on large3_id = small2_id " +
        "left outer join small3 on large3_id = small3_id ";

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

    //(((((default.large1 ⟕ default.large2) ⟕ default.small1) ⟕ default.large3) ⟕ default.small2) ⟕ default.small3)
    /*
    |-eb_1402634570910_0000_000007
       |-eb_1402634570910_0000_000006            (GROUP BY)
          |-eb_1402634570910_0000_000005         (JOIN, broadcast small2, small3)
             |-eb_1402634570910_0000_000004      (LEAF, scan large3)
             |-eb_1402634570910_0000_000003      (JOIN, broadcast small1)
                |-eb_1402634570910_0000_000002   (LEAF, scan large2)
                |-eb_1402634570910_0000_000001   (LEAF, scan large1)
    */
    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.SCAN, node.getType());
        ScanNode scanNode = (ScanNode)node;
        assertEquals("default.large1", scanNode.getCanonicalName());

        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(0, broadcastTables.size());
      } else if (index == 1) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.SCAN, node.getType());
        ScanNode scanNode = (ScanNode)node;
        assertEquals("default.large2", scanNode.getCanonicalName());

        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(0, broadcastTables.size());
      } else if(index == 2) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.JOIN, node.getType());
        JoinNode joinNode = (JoinNode)node;

        ScanNode leftNode = ((JoinNode)joinNode.getLeftChild()).getLeftChild();
        ScanNode rightNode = ((JoinNode)joinNode.getLeftChild()).getRightChild();
        assertTrue(leftNode.getCanonicalName().indexOf("0000_000001") > 0);
        assertTrue(rightNode.getCanonicalName().indexOf("0000_000002") > 0);

        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(1, broadcastTables.size());
        assertTrue(broadcastTables.contains("default.small1"));
      } else if(index == 3) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.SCAN, node.getType());
        ScanNode scanNode = (ScanNode)node;
        assertEquals("default.large3", scanNode.getCanonicalName());

        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(0, broadcastTables.size());
      } else if(index == 4) {
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(2, broadcastTables.size());
        assertTrue(broadcastTables.contains("default.small2"));
        assertTrue(broadcastTables.contains("default.small3"));
      }
      index++;
    }

    assertEquals(7, index);
  }

  @Test
  public final void testLeftOuterJoinCase4() throws IOException, PlanningException {
    // small1, small2, small3
    String query = "select count(*) from small1 " +
        "left outer join small2 on small1_id = small2_id " +
        "left outer join small3 on small1_id = small3_id ";

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
    |-eb_1402500846700_0000_000007
       |-eb_1402500846700_0000_000006
          |-eb_1402500846700_0000_000005 (LEAF, broadcast join small1, small2, small3)
    */

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        GroupbyNode node = (GroupbyNode)eb.getPlan();
        JoinNode joinNode = node.getChild();

        ScanNode scanNode = joinNode.getRightChild();
        assertEquals("default.small3", scanNode.getCanonicalName());

        joinNode = joinNode.getLeftChild();
        scanNode = joinNode.getLeftChild();
        assertEquals("default.small1", scanNode.getCanonicalName());
        scanNode = joinNode.getRightChild();
        assertEquals("default.small2", scanNode.getCanonicalName());

        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(3, broadcastTables.size());
      } else if(index == 1) {
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(0, broadcastTables.size());
      }
      index++;
    }

    assertEquals(3, index);
  }

  @Test
  public final void testLeftOuterJoinCase5() throws IOException, PlanningException {
    // small, small, large, small
    String query = "select count(*) from small1 " +
        "left outer join small2 on small1_id = small2_id " +
        "left outer join large1 on small1_id = large1_id " +
        "left outer join small3 on small1_id = small3_id " ;

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

    //(((default.small1 ⟕ default.small2) ⟕ default.large1) ⟕ default.small3)
    /*
     |-eb_1402642709028_0000_000005
       |-eb_1402642709028_0000_000004    (GROUP BY)
          |-eb_1402642709028_0000_000003 (LEAF, broadcast JOIN small1, small2, small3, large1)
     */

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(3, broadcastTables.size());

        assertTrue(broadcastTables.contains("default.small1"));
        assertTrue(broadcastTables.contains("default.small2"));
        assertTrue(broadcastTables.contains("default.small3"));
      } else if(index == 1 || index == 2 || index == 3) {
        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(0, broadcastTables.size());
      }
      index++;
    }

    assertEquals(3, index);
  }

  @Test
  public final void testLeftOuterJoinCase6() throws IOException, PlanningException {
    // small1, small2, large1, large2, small3
    String query = "select count(*) from small1 " +
        "left outer join small2 on small1_id = small2_id " +
        "left outer join large1 on small1_id = large1_id " +
        "left outer join large2 on small1_id = large2_id " +
        "left outer join small3 on small1_id = small3_id " ;

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

    // ((((default.small1 ⟕ default.small2) ⟕ default.large1) ⟕ default.large2) ⟕ default.small3)

    /*
    |-eb_1404125948432_0000_000007
       |-eb_1404125948432_0000_000006
          |-eb_1404125948432_0000_000005     (JOIN broadcast small3)
             |-eb_1404125948432_0000_000004  (LEAF, scan large2)
             |-eb_1404125948432_0000_000003  (LEAF, scan large1, broadcast small1, small2)
    */
    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.JOIN, node.getType());
        JoinNode joinNode = (JoinNode)node;

        JoinNode joinNode2 = joinNode.getLeftChild();
        ScanNode scanNode2 = joinNode.getRightChild();
        assertEquals("default.large1", scanNode2.getCanonicalName());

        ScanNode scanNode3 = joinNode2.getLeftChild();
        ScanNode scanNode4 = joinNode2.getRightChild();
        assertEquals("default.small1", scanNode3.getCanonicalName());
        assertEquals("default.small2", scanNode4.getCanonicalName());

        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(2, broadcastTables.size());
      } else if (index == 1) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.SCAN, node.getType());
        ScanNode scanNode = (ScanNode)node;
        assertEquals("default.large2", scanNode.getCanonicalName());

        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(0, broadcastTables.size());
      } else if(index == 2) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.GROUP_BY, node.getType());

        JoinNode joinNode = ((GroupbyNode)node).getChild();

        JoinNode joinNode1 = joinNode.getLeftChild();
        ScanNode scanNode1 = joinNode.getRightChild();
        assertEquals("default.small3", scanNode1.getCanonicalName());

        ScanNode scanNode2 = joinNode1.getLeftChild();
        ScanNode scanNode3 = joinNode1.getRightChild();
        assertTrue(scanNode2.getCanonicalName().indexOf("0000_000003") > 0);
        assertTrue(scanNode3.getCanonicalName().indexOf("0000_000004") > 0);

        Collection<String> broadcastTables = eb.getBroadcastTables();
        assertEquals(1, broadcastTables.size());
      }
      index++;
    }

    assertEquals(5, index);
  }

  @Test
  public final void testInnerLeftOuterJoinCase1() throws IOException, PlanningException {
    // small, small, large, small
    String query = "select count(*) from small1 " +
        "inner join small2 on small1_id = small2_id " +
        "left outer join large1 on small1_id = large1_id " +
        "left outer join small3 on small3_id = large1_id " ;

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

    // (((default.small1 ⋈θ default.small2) ⟕ default.large1) ⟕ default.small3)
    /*
    |-eb_1404139312268_0000_000006
       |-eb_1404139312268_0000_000005
          |-eb_1404139312268_0000_000003 (LEAF scan large1, broadcast small1, small2, small3)
     */

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.GROUP_BY, node.getType());
        JoinNode joinNode = ((GroupbyNode)node).getChild();

        JoinNode joinNode2 = joinNode.getLeftChild();
        ScanNode scanNode = joinNode.getRightChild();
        assertEquals("default.small3", scanNode.getCanonicalName());

        JoinNode joinNode3 = joinNode2.getLeftChild();
        ScanNode scanNode2 = joinNode2.getRightChild();

        assertEquals("default.large1", scanNode2.getCanonicalName());

        ScanNode scanNode3 = joinNode3.getLeftChild();
        ScanNode scanNode4 = joinNode3.getRightChild();

        assertEquals("default.small1", scanNode3.getCanonicalName());
        assertEquals("default.small2", scanNode4.getCanonicalName());

        Collection<String> broadcastTables = eb.getBroadcastTables();

        assertEquals(3, broadcastTables.size());
        assertTrue(broadcastTables.contains("default.small1"));
        assertTrue(broadcastTables.contains("default.small2"));
        assertTrue(broadcastTables.contains("default.small3"));
      }
      index++;
    }

    assertEquals(3, index);
  }

  @Test
  public final void testBroadcastCasebyCase1() throws IOException, PlanningException {
    // large, small, large, small
    String query = "select count(*) from large1 " +
        "inner join small1 on large1_id = small1_id " +
        "left outer join large2 on large1_id = large2_id " +
        "left outer join small2 on large1_id = small2_id " ;

    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalOptimizer optimizer = new LogicalOptimizer(conf);
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext();
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(masterPlan);

    // (((default.large1 ⋈θ default.small1) ⟕ default.large2) ⟕ default.small2)
    /*
    |-eb_1404871198908_0000_000007
      |-eb_1404871198908_0000_000006
        |-eb_1404871198908_0000_000005   (join eb3, eb3, broadcast small2)
          |-eb_1404871198908_0000_000004 (scan large2)
          |-eb_1404871198908_0000_000003 (scan large1, broadcast small1)
    */

    ExecutionBlockCursor ebCursor = new ExecutionBlockCursor(masterPlan);
    int index = 0;
    while (ebCursor.hasNext()) {
      ExecutionBlock eb = ebCursor.nextBlock();
      if(index == 0) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.JOIN, node.getType());
        JoinNode joinNode = (JoinNode)node;

        ScanNode scanNode1 = joinNode.getLeftChild();
        ScanNode scanNode2 = joinNode.getRightChild();
        assertEquals("default.large1", scanNode1.getCanonicalName());
        assertEquals("default.small1", scanNode2.getCanonicalName());

        Collection<String> broadcastTables = eb.getBroadcastTables();

        assertEquals(1, broadcastTables.size());
        assertTrue(broadcastTables.contains("default.small1"));
      } else if(index == 1) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.SCAN, node.getType());
        ScanNode scanNode = (ScanNode)node;

        assertEquals("default.large2", scanNode.getCanonicalName());

        Collection<String> broadcastTables = eb.getBroadcastTables();
        TestCase.assertEquals(0, broadcastTables.size());
      } else if(index == 2) {
        LogicalNode node = eb.getPlan();
        assertEquals(NodeType.GROUP_BY, node.getType());
        JoinNode joinNode = ((GroupbyNode)node).getChild();

        JoinNode joinNode2 = joinNode.getLeftChild();
        ScanNode scanNode = joinNode.getRightChild();
        assertEquals("default.small2", scanNode.getCanonicalName());

        ScanNode scanNode2 = joinNode2.getLeftChild();
        ScanNode scanNode3 = joinNode2.getRightChild();

        assertTrue(scanNode2.getCanonicalName().indexOf("000003") >= 0);
        assertTrue(scanNode3.getCanonicalName().indexOf("000004") >= 0);

        Collection<String> broadcastTables = eb.getBroadcastTables();

        TestCase.assertEquals(1, broadcastTables.size());
        TestCase.assertTrue(broadcastTables.contains("default.small2"));
      }
      index++;
    }

    TestCase.assertEquals(5, index);
  }
}
