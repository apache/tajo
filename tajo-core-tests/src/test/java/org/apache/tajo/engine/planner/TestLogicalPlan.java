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
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.graph.SimpleDirectedGraph;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.apache.tajo.plan.LogicalPlan.BlockType;
import static org.junit.Assert.*;

public class TestLogicalPlan {
  private static TajoTestingCluster util;
  private static LogicalPlanner planner;

  @BeforeClass
  public static void setup() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    planner = new LogicalPlanner(util.getMiniCatalogCluster().getCatalog(), TablespaceManager.getInstance());
  }

  public static void tearDown() {
    util.shutdownCatalogCluster();
  }

  @Test
  public final void testQueryBlockGraph() {
    LogicalPlan plan = new LogicalPlan(planner);
    LogicalPlan.QueryBlock root = plan.newAndGetBlock(LogicalPlan.ROOT_BLOCK);
    LogicalPlan.QueryBlock new1 = plan.newQueryBlock();
    LogicalPlan.QueryBlock new2 = plan.newQueryBlock();

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
