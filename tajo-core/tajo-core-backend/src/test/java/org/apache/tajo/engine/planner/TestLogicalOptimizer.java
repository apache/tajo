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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.function.builtin.SumInt;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.master.TajoMaster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestLogicalOptimizer {

  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer sqlAnalyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.registerFunction(funcDesc);
    }
    
    Schema schema = new Schema();
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("empId", Type.INT4);
    schema.addColumn("deptName", Type.TEXT);

    Schema schema2 = new Schema();
    schema2.addColumn("deptName", Type.TEXT);
    schema2.addColumn("manager", Type.TEXT);

    Schema schema3 = new Schema();
    schema3.addColumn("deptName", Type.TEXT);
    schema3.addColumn("score", Type.INT4);
    schema3.addColumn("phone", Type.INT4);

    TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc people = new TableDescImpl("employee", meta,
        new Path("file:///"));
    catalog.addTable(people);

    TableDesc student = new TableDescImpl("dept", schema2, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(student);

    TableDesc score = new TableDescImpl("score", schema3, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(score);

    FunctionDesc funcDesc = new FunctionDesc("sumtest", SumInt.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4));

    catalog.registerFunction(funcDesc);
    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }
  
  static String[] QUERIES = {
    "select name, manager from employee as e, dept as dp where e.deptName = dp.deptName", // 0
    "select name, empId, deptName from employee where empId > 500", // 1
    "select name from employee where empId = 100", // 2
    "select name, max(empId) as final from employee where empId > 50 group by name", // 3
    "select name, score from employee natural join score", // 4
    "select name, score from employee join score on employee.deptName = score.deptName", // 5
  };
  
  @Test
  public final void testProjectionPushWithNaturalJoin() throws PlanningException, CloneNotSupportedException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[4]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();    
    assertEquals(ExprType.JOIN, projNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) projNode.getSubNode();
    assertEquals(ExprType.SCAN, joinNode.getOuterNode().getType());
    assertEquals(ExprType.SCAN, joinNode.getInnerNode().getType());
    
    LogicalNode optimized = optimizer.optimize(newPlan);

    assertEquals(ExprType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.JOIN, root.getSubNode().getType());
    joinNode = (JoinNode) root.getSubNode();
    assertEquals(ExprType.SCAN, joinNode.getOuterNode().getType());
    assertEquals(ExprType.SCAN, joinNode.getInnerNode().getType());
  }
  
  @Test
  public final void testProjectionPushWithInnerJoin() throws PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[5]);
    LogicalPlan newPlan = planner.createPlan(expr);
    optimizer.optimize(newPlan);
  }
  
  @Test
  public final void testProjectionPush() throws CloneNotSupportedException, PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[2]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();
    assertEquals(ExprType.SELECTION, projNode.getSubNode().getType());
    SelectionNode selNode = (SelectionNode) projNode.getSubNode();    
    assertEquals(ExprType.SCAN, selNode.getSubNode().getType());        
    
    LogicalNode optimized = optimizer.optimize(newPlan);
    assertEquals(ExprType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.SCAN, root.getSubNode().getType());
  }
  
  @Test
  public final void testOptimizeWithGroupBy() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[3]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
        
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();
    assertEquals(ExprType.GROUP_BY, projNode.getSubNode().getType());
    GroupbyNode groupbyNode = (GroupbyNode) projNode.getSubNode();
    assertEquals(ExprType.SELECTION, groupbyNode.getSubNode().getType());
    SelectionNode selNode = (SelectionNode) groupbyNode.getSubNode();
    assertEquals(ExprType.SCAN, selNode.getSubNode().getType());

    System.out.println(newPlan.getRootBlock().getRoot());
    LogicalNode optimized = optimizer.optimize(newPlan);
    assertEquals(ExprType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.GROUP_BY, root.getSubNode().getType());
    groupbyNode = (GroupbyNode) root.getSubNode();    
    assertEquals(ExprType.SCAN, groupbyNode.getSubNode().getType());
  }

  @Test
  public final void testPushable() throws CloneNotSupportedException, PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[0]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();

    assertEquals(ExprType.SELECTION, projNode.getSubNode().getType());
    SelectionNode selNode = (SelectionNode) projNode.getSubNode();
    
    assertEquals(ExprType.JOIN, selNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) selNode.getSubNode();
    assertFalse(joinNode.hasJoinQual());
    
    // Test for Pushable
    assertTrue(PlannerUtil.canBeEvaluated(selNode.getQual(), joinNode));
    
    // Optimized plan
    LogicalNode optimized = optimizer.optimize(newPlan);
    assertEquals(ExprType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    
    assertEquals(ExprType.JOIN, root.getSubNode().getType());
    joinNode = (JoinNode) root.getSubNode();
    assertTrue(joinNode.hasJoinQual());
    
    // Scan Pushable Test
    expr = sqlAnalyzer.parse(QUERIES[1]);
    newPlan = planner.createPlan(expr);
    plan = newPlan.getRootBlock().getRoot();
    
    assertEquals(ExprType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    projNode = (ProjectionNode) root.getSubNode();

    assertEquals(ExprType.SELECTION, projNode.getSubNode().getType());
    selNode = (SelectionNode) projNode.getSubNode();
    
    assertEquals(ExprType.SCAN, selNode.getSubNode().getType());
    ScanNode scanNode = (ScanNode) selNode.getSubNode();
    // Test for Join Node
    assertTrue(PlannerUtil.canBeEvaluated(selNode.getQual(), scanNode));
  }
}
