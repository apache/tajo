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

package tajo.engine.planner;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.TajoTestingCluster;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.engine.function.builtin.SumInt;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.logical.*;
import tajo.master.TajoMaster;

import static org.junit.Assert.*;

public class TestLogicalOptimizer {

  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static QueryAnalyzer analyzer;
  private static LogicalPlanner planner;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.registerFunction(funcDesc);
    }
    
    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);

    Schema schema2 = new Schema();
    schema2.addColumn("deptName", DataType.STRING);
    schema2.addColumn("manager", DataType.STRING);

    Schema schema3 = new Schema();
    schema3.addColumn("deptName", DataType.STRING);
    schema3.addColumn("score", DataType.INT);
    schema3.addColumn("phone", DataType.INT);

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
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
        new DataType[] {DataType.INT},
        new DataType[] {DataType.INT});

    catalog.registerFunction(funcDesc);
    analyzer = new QueryAnalyzer(catalog);
    planner = new LogicalPlanner(catalog);
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
  public final void testProjectionPushWithNaturalJoin() throws CloneNotSupportedException {
    // two relations
    PlanningContext context = analyzer.parse(QUERIES[4]);
    LogicalNode plan = planner.createPlan(context);
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();    
    assertEquals(ExprType.JOIN, projNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) projNode.getSubNode();
    assertEquals(ExprType.SCAN, joinNode.getOuterNode().getType());
    assertEquals(ExprType.SCAN, joinNode.getInnerNode().getType());
    
    LogicalNode optimized = LogicalOptimizer.optimize(context, plan);
    assertEquals(ExprType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.JOIN, root.getSubNode().getType());
    joinNode = (JoinNode) root.getSubNode();
    assertEquals(ExprType.SCAN, joinNode.getOuterNode().getType());
    assertEquals(ExprType.SCAN, joinNode.getInnerNode().getType());
  }
  
  @Test
  public final void testProjectionPushWithInnerJoin() throws CloneNotSupportedException {
    // two relations
    PlanningContext context = analyzer.parse(QUERIES[5]);
    LogicalNode plan = planner.createPlan(context);
    System.out.println(plan);
    System.out.println("--------------");
    LogicalNode optimized = LogicalOptimizer.optimize(context, plan);
    System.out.println(optimized);
  }
  
  @Test
  public final void testProjectionPush() throws CloneNotSupportedException {
    // two relations
    PlanningContext context = analyzer.parse(QUERIES[2]);
    LogicalNode plan = planner.createPlan(context);
    
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();
    assertEquals(ExprType.SELECTION, projNode.getSubNode().getType());
    SelectionNode selNode = (SelectionNode) projNode.getSubNode();    
    assertEquals(ExprType.SCAN, selNode.getSubNode().getType());        
    
    LogicalNode optimized = LogicalOptimizer.optimize(context, plan);
    assertEquals(ExprType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.SCAN, root.getSubNode().getType());
  }
  
  @Test
  public final void testOptimizeWithGroupBy() throws CloneNotSupportedException {
    PlanningContext context = analyzer.parse(QUERIES[3]);
    LogicalNode plan = planner.createPlan(context);
        
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
    
    LogicalNode optimized = LogicalOptimizer.optimize(context, plan);
    assertEquals(ExprType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(ExprType.GROUP_BY, root.getSubNode().getType());
    groupbyNode = (GroupbyNode) root.getSubNode();    
    assertEquals(ExprType.SCAN, groupbyNode.getSubNode().getType());
  }

  @Test
  public final void testPushable() throws CloneNotSupportedException {
    // two relations
    PlanningContext context = analyzer.parse(QUERIES[0]);
    LogicalNode plan = planner.createPlan(context);
    
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
    assertTrue(LogicalOptimizer.canBeEvaluated(selNode.getQual(), joinNode));
    
    // Optimized plan
    LogicalNode optimized = LogicalOptimizer.optimize(context, plan);
    assertEquals(ExprType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    
    assertEquals(ExprType.JOIN, root.getSubNode().getType());
    joinNode = (JoinNode) root.getSubNode();
    assertTrue(joinNode.hasJoinQual());
    
    // Scan Pushable Test
    context = analyzer.parse(QUERIES[1]);
    plan = planner.createPlan(context);
    
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
    assertTrue(LogicalOptimizer.canBeEvaluated(selNode.getQual(), scanNode));
  }
}
