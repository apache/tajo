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

package org.apache.tajo.engine.planner;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.*;

public class TestGreedyHeuristicJoinOrderAlgorithm {

  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer sqlAnalyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static QueryContext defaultContext;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : FunctionLoader.findLegacyFunctions()) {
      catalog.createFunction(funcDesc);
    }

    Schema schema = new Schema();
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("empid", Type.INT4);
    schema.addColumn("deptname", Type.TEXT);

    Schema schema2 = new Schema();
    schema2.addColumn("deptname", Type.TEXT);
    schema2.addColumn("manager", Type.TEXT);

    Schema schema3 = new Schema();
    schema3.addColumn("deptname", Type.TEXT);
    schema3.addColumn("score", Type.INT4);
    schema3.addColumn("phone", Type.INT4);

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
    TableDesc people = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, meta,
        CommonTestingUtil.getTestDir().toUri());
    catalog.createTable(people);

    TableDesc student =
        new TableDesc(
            CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), schema2, StoreType.CSV, new KeyValueSet(),
            CommonTestingUtil.getTestDir().toUri());
    catalog.createTable(student);

    TableDesc score =
        new TableDesc(
            CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), schema3, StoreType.CSV, new KeyValueSet(),
            CommonTestingUtil.getTestDir().toUri());
    catalog.createTable(score);

    ///////////////////////////////////////////////////////////////////////////
    // creating table for overflow in JoinOrderOptimizer.
    Schema schema4 = new Schema();
    schema4.addColumn("deptname", Type.TEXT);
    schema4.addColumn("manager", Type.TEXT);
    //If table's store type is StoreType.SYSTEM, Planner doesn't update table stats.
    TableMeta largeTableMeta = CatalogUtil.newTableMeta(StoreType.SYSTEM);
    TableDesc largeDept =
        new TableDesc(
            CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "large_dept"), schema4, StoreType.CSV, new KeyValueSet(),
            CommonTestingUtil.getTestDir().toUri());
    largeDept.setMeta(largeTableMeta);
    TableStats largeTableStats = new TableStats();
    largeTableStats.setNumBytes(1024L * 1024L * 1024L * 1024L *  1024L);  //1 PB
//    largeTableStats.setNumBytes(1024L * 1024L);  //1 PB
    largeDept.setStats(largeTableStats);
    catalog.createTable(largeDept);
    ///////////////////////////////////////////////////////////////////////////

    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(util.getConfiguration());

    defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  @Test
  public final void testFindBestJoinOrder() throws Exception {
    String query = "select e.name, d.manager from employee as e, dept as d, score as s, large_dept as ld " +
        "where e.deptName = d.deptName and d.deptname = s.deptname and s.deptname=ld.deptname";
    Expr expr = sqlAnalyzer.parse(query);
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);

    //Not optimized plan has 3 join nodes
    // [[employee-dept]-score]-large_dept
    LogicalNode[] joinNodes = PlannerUtil.findAllNodes(newPlan. getRootBlock().getRoot() , NodeType.JOIN);
    assertNotNull(joinNodes);
    assertEquals(3, joinNodes.length);
    assertJoinNode(joinNodes[0], "default.e", "default.d");
    assertJoinNode(joinNodes[1], null, "default.s");
    assertJoinNode(joinNodes[2], null, "default.ld");

    optimizer.optimize(newPlan);

    //Optimized plan has 3 join nodes
    // [employee-dept]-[score-large_dept]
    joinNodes = PlannerUtil.findAllNodes(newPlan. getRootBlock().getRoot() , NodeType.JOIN);
    assertNotNull(joinNodes);
    assertEquals(3, joinNodes.length);
    assertJoinNode(joinNodes[0], "default.e", "default.d");
    assertJoinNode(joinNodes[1], "default.s", "default.ld");
    assertJoinNode(joinNodes[2], null, null);
  }

  @Test
  public final void testCheckingInfinityJoinScore() throws Exception {
    // Test for TAJO-1552
    String query = "select a.deptname from large_dept a, large_dept b, large_dept c, " +
        "large_dept d, large_dept e, large_dept f ";

    Expr expr = sqlAnalyzer.parse(query);
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode[] joinNodes = PlannerUtil.findAllNodes(newPlan. getRootBlock().getRoot() , NodeType.JOIN);
    assertNotNull(joinNodes);
    assertEquals(5, joinNodes.length);
    assertJoinNode(joinNodes[0], "default.a", "default.b");
    assertJoinNode(joinNodes[1], null, "default.c");
    assertJoinNode(joinNodes[2], null, "default.d");
    assertJoinNode(joinNodes[3], null, "default.e");
    assertJoinNode(joinNodes[4], null, "default.f");

    optimizer.optimize(newPlan);

    joinNodes = PlannerUtil.findAllNodes(newPlan. getRootBlock().getRoot() , NodeType.JOIN);
    assertNotNull(joinNodes);
    assertEquals(5, joinNodes.length);
    assertJoinNode(joinNodes[0], "default.e", "default.f");
    assertJoinNode(joinNodes[1], "default.a", "default.b");
    assertJoinNode(joinNodes[2], "default.c", "default.d");
    assertJoinNode(joinNodes[3], null, null);
    assertJoinNode(joinNodes[4], null, null);

  }

  private void assertJoinNode(LogicalNode node, String left, String right) {
    assertEquals(NodeType.JOIN, node.getType());
    JoinNode joinNode = (JoinNode)node;

    if (left != null) {
      assertEquals(left, ((ScanNode)joinNode.getLeftChild()).getCanonicalName());
    } else {
      assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());
    }

    if (right != null) {
      assertEquals(right, ((ScanNode)joinNode.getRightChild()).getCanonicalName());
    } else {
      assertEquals(NodeType.JOIN, joinNode.getRightChild().getType());
    }
  }
}
