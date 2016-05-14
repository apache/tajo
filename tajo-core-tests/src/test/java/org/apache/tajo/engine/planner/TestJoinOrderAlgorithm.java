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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestJoinOrderAlgorithm {

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
    catalog = util.getCatalogService();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : FunctionLoader.findLegacyFunctions()) {
      catalog.createFunction(funcDesc);
    }

    Schema schema = SchemaBuilder.builder()
        .add("name", Type.TEXT)
        .add("empid", Type.INT4)
        .add("deptname", Type.TEXT)
        .build();

    Schema schema2 = SchemaBuilder.builder()
        .add("deptname", Type.TEXT)
        .add("manager", Type.TEXT)
        .build();

    Schema schema3 = SchemaBuilder.builder()
        .add("deptname", Type.TEXT)
        .add("score", Type.INT4)
        .add("phone", Type.INT4)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration());
    TableDesc people = new TableDesc(
        IdentifierUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, meta,
        CommonTestingUtil.getTestDir().toUri());
    catalog.createTable(people);

    TableDesc student =
        new TableDesc(
            IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), schema2, "TEXT", new KeyValueSet(),
            CommonTestingUtil.getTestDir().toUri());
    catalog.createTable(student);

    TableDesc score =
        new TableDesc(
            IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), schema3, "TEXT", new KeyValueSet(),
            CommonTestingUtil.getTestDir().toUri());
    catalog.createTable(score);

    ///////////////////////////////////////////////////////////////////////////
    // creating table for overflow in JoinOrderOptimizer.
    Schema schema4 = SchemaBuilder.builder()
        .add("deptname", Type.TEXT)
        .add("manager", Type.TEXT)
        .build();
    // Set store type as FAKEFILE to prevent auto update of physical information in LogicalPlanner.updatePhysicalInfo()
    TableMeta largeTableMeta = CatalogUtil.newTableMeta("FAKEFILE", util.getConfiguration());
    TableDesc largeDept;
    TableStats largeTableStats;
    FileSystem fs = FileSystem.getLocal(util.getConfiguration());
    for (int i = 0; i < 6; i++) {
      Path tablePath = new Path(CommonTestingUtil.getTestDir(), "" + (i+1));
      fs.create(tablePath);
      largeDept =
          new TableDesc(
              IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "large_dept"+(i+1)), schema4, largeTableMeta,
              tablePath.toUri());
      largeTableStats = new TableStats();
      largeTableStats.setNumBytes(StorageUnit.PB * (i+1));  //1 PB * i
      largeDept.setStats(largeTableStats);
      catalog.createTable(largeDept);
    }
    ///////////////////////////////////////////////////////////////////////////

    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());

    defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  @Test
  public final void testCheckingInfinityJoinScore() throws Exception {
    // Test for TAJO-1552
    String query = "select a.deptname from large_dept1 a, large_dept2 b, large_dept3 c, " +
        "large_dept4 d, large_dept5 e, large_dept6 f ";

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
    assertJoinNode(joinNodes[0], "default.d", "default.c");
    assertJoinNode(joinNodes[1], "default.b", "default.a");
    assertJoinNode(joinNodes[2], null, null);
    assertJoinNode(joinNodes[3], "default.f", "default.e");
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
