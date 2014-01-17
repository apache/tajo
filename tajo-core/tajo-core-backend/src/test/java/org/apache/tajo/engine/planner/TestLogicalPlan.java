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

import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.graph.SimpleDirectedGraph;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.apache.tajo.engine.planner.LogicalPlan.BlockType;
import static org.junit.Assert.*;

public class TestLogicalPlan {
  private static TajoTestingCluster util;
  private static TPCH tpch;
  private static CatalogService catalog;
  private static SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;

  @BeforeClass
  public static void setup() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }

    // TPC-H Schema for Complex Queries
    String [] tpchTables = {
        "part", "supplier", "partsupp", "nation", "region", "lineitem", "customer", "orders"
    };
    int [] tableVolumns = {
        100, 200, 50, 5, 5, 800, 300, 100
    };
    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();

    for (int i = 0; i < tpchTables.length; i++) {
      TableMeta m = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
      TableStats stats = new TableStats();
      stats.setNumBytes(tableVolumns[i]);
      TableDesc d = CatalogUtil.newTableDesc(tpchTables[i], tpch.getSchema(tpchTables[i]), m,
          CommonTestingUtil.getTestDir());
      d.setStats(stats);
      catalog.addTable(d);
    }
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(util.getConfiguration());
  }

  public static void tearDown() {
    util.shutdownCatalogCluster();
  }

  @Test
  public final void testQueryBlockGraph() {
    LogicalPlan plan = new LogicalPlan(planner);
    LogicalPlan.QueryBlock root = plan.newAndGetBlock(LogicalPlan.ROOT_BLOCK);
    LogicalPlan.QueryBlock new1 = plan.newAndGetBlock("@new1");
    LogicalPlan.QueryBlock new2 = plan.newAndGetBlock("@new2");

    plan.getQueryBlockGraph().addEdge(new1.getName(), root.getName(),
        new LogicalPlan.BlockEdge(new1, root, BlockType.TableSubQuery));
    plan.getQueryBlockGraph().addEdge(new2.getName(), root.getName(),
        new LogicalPlan.BlockEdge(new2, root, BlockType.TableSubQuery));

    SimpleDirectedGraph<String, LogicalPlan.BlockEdge> graph = plan.getQueryBlockGraph();
    assertEquals(2, graph.getChildCount(root.getName()));

    assertEquals(root.getName(), graph.getParent(new1.getName(), 0));
    assertEquals(root.getName(), graph.getParent(new2.getName(), 0));

    assertTrue(graph.isRoot(root.getName()));
    assertFalse(graph.isRoot(new1.getName()));
    assertFalse(graph.isRoot(new2.getName()));

    assertFalse(graph.isLeaf(root.getName()));
    assertTrue(graph.isLeaf(new1.getName()));
    assertTrue(graph.isLeaf(new2.getName()));

    Set<LogicalPlan.QueryBlock> result = new HashSet<LogicalPlan.QueryBlock>();
    result.add(new1);
    result.add(new2);

    Set<LogicalPlan.QueryBlock> childs = new HashSet<LogicalPlan.QueryBlock>(plan.getChildBlocks(root));
    assertEquals(result, childs);

    assertEquals(root, plan.getParentBlock(new1));
    assertEquals(root, plan.getParentBlock(new2));
  }
}
