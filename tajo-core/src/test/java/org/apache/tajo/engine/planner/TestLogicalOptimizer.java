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
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.function.builtin.SumInt;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.*;

public class TestLogicalOptimizer {

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
        CommonTestingUtil.getTestDir());
    catalog.createTable(people);

    TableDesc student =
        new TableDesc(
            CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), schema2, StoreType.CSV, new KeyValueSet(),
            CommonTestingUtil.getTestDir());
    catalog.createTable(student);

    TableDesc score =
        new TableDesc(
            CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), schema3, StoreType.CSV, new KeyValueSet(),
            CommonTestingUtil.getTestDir());
    catalog.createTable(score);

    FunctionDesc funcDesc = new FunctionDesc("sumtest", SumInt.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4));

    catalog.createFunction(funcDesc);
    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(util.getConfiguration());

    defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
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
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();
    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    JoinNode joinNode = projNode.getChild();
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType());
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
    
    LogicalNode optimized = optimizer.optimize(newPlan);

    assertEquals(NodeType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.JOIN, root.getChild().getType());
    joinNode = root.getChild();
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType());
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
  }
  
  @Test
  public final void testProjectionPushWithInnerJoin() throws PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[5]);
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    optimizer.optimize(newPlan);
  }
  
  @Test
  public final void testProjectionPush() throws CloneNotSupportedException, PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[2]);
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = projNode.getChild();
    assertEquals(NodeType.SCAN, selNode.getChild().getType());

    LogicalNode optimized = optimizer.optimize(newPlan);
    assertEquals(NodeType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.SCAN, root.getChild().getType());
  }
  
  @Test
  public final void testOptimizeWithGroupBy() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[3]);
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
        
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();
    assertEquals(NodeType.GROUP_BY, projNode.getChild().getType());
    GroupbyNode groupbyNode = projNode.getChild();
    assertEquals(NodeType.SELECTION, groupbyNode.getChild().getType());
    SelectionNode selNode = groupbyNode.getChild();
    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    
    LogicalNode optimized = optimizer.optimize(newPlan);
    assertEquals(NodeType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.GROUP_BY, root.getChild().getType());
    groupbyNode = root.getChild();
    assertEquals(NodeType.SCAN, groupbyNode.getChild().getType());
  }

  @Test
  public final void testPushable() throws CloneNotSupportedException, PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[0]);
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = projNode.getChild();
    
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = selNode.getChild();
    assertFalse(joinNode.hasJoinQual());
    
    // Test for Pushable
    assertTrue(LogicalPlanner.checkIfBeEvaluatedAtJoin(newPlan.getRootBlock(), selNode.getQual(), joinNode, false));
    
    // Optimized plan
    LogicalNode optimized = optimizer.optimize(newPlan);
    assertEquals(NodeType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    
    assertEquals(NodeType.JOIN, root.getChild().getType());
    joinNode = root.getChild();
    assertTrue(joinNode.hasJoinQual());
    
    // Scan Pushable Test
    expr = sqlAnalyzer.parse(QUERIES[1]);
    newPlan = planner.createPlan(defaultContext, expr);
    plan = newPlan.getRootBlock().getRoot();
    
    assertEquals(NodeType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    projNode = root.getChild();

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    selNode = projNode.getChild();
    
    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    // Test for Join Node
    assertTrue(LogicalPlanner.checkIfBeEvaluatedAtRelation(newPlan.getRootBlock(), selNode.getQual(), scanNode));
  }

  @Test
  public final void testInsertInto() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(TestLogicalPlanner.insertStatements[0]);
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    optimizer.optimize(newPlan);
  }
}
