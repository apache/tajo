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
import org.apache.tajo.QueryVars;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.*;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.*;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.util.EvalNodeToExprConverter;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.session.Session;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Stack;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.*;

public class TestEvalNodeToExprConverter {
  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer sqlAnalyzer;
  private static LogicalPlanner planner;
  private static TPCH tpch;
  private static Session session = LocalTajoTestingUtility.createDummySession();

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getCatalogService();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234");
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);

    for (FunctionDesc funcDesc : FunctionLoader.findLegacyFunctions()) {
      catalog.createFunction(funcDesc);
    }

    // TPC-H Schema for Complex Queries
    String [] tpchTables = {
      "part", "supplier", "partsupp", "nation", "region", "lineitem"
    };
    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();
    for (String table : tpchTables) {
      TableMeta m = CatalogUtil.newTableMeta("TEXT");
      TableDesc d = CatalogUtil.newTableDesc(
        CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, table), tpch.getSchema(table), m,
        CommonTestingUtil.getTestDir());
      catalog.createTable(d);
    }

    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  static String[] QUERIES = {
    "select * from lineitem where L_ORDERKEY > 500", //0
    "select * from region where r_name = 'EUROPE'", //1
    "select * from lineitem where L_DISCOUNT >= 0.05 and L_DISCOUNT <= 0.07 OR L_QUANTITY < 24.0 ", //2
    "select * from lineitem where L_DISCOUNT between 0.06 - 0.01 and 0.08 + 0.02 and L_ORDERKEY < 24 ", //3
    "select * from lineitem where (case when L_DISCOUNT > 0.0 then L_DISCOUNT / L_TAX else null end) > 1.2 ", //4
    "select * from part where p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') " +
      "and p_size between 1 and 10", //5
  };

  private static QueryContext createQueryContext() {
    QueryContext qc = new QueryContext(util.getConfiguration(), session);
    qc.put(QueryVars.DEFAULT_SPACE_URI, "file:/");
    qc.put(QueryVars.DEFAULT_SPACE_ROOT_URI, "file:/");
    return qc;
  }

  @Test
  public final void testBinaryOperator1() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[0]);

    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    LogicalNode node = plan.getRootBlock().getRoot();
    ScanNode scanNode = PlannerUtil.findTopNode(node, NodeType.SCAN);

    EvalNodeToExprConverter convertor = new EvalNodeToExprConverter(scanNode.getTableName());
    convertor.visit(null, scanNode.getQual(), new Stack<>());

    Expr resultExpr = convertor.getResult();

    BinaryOperator binaryOperator = AlgebraicUtil.findTopExpr(resultExpr, OpType.GreaterThan);
    assertNotNull(binaryOperator);

    ColumnReferenceExpr column = binaryOperator.getLeft();
    assertEquals("default.lineitem", column.getQualifier());
    assertEquals("l_orderkey", column.getName());

    LiteralValue literalValue = binaryOperator.getRight();
    assertEquals("500", literalValue.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Integer, literalValue.getValueType());
  }

  @Test
  public final void testBinaryOperator2() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[1]);

    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    LogicalNode node = plan.getRootBlock().getRoot();
    ScanNode scanNode = PlannerUtil.findTopNode(node, NodeType.SCAN);

    EvalNodeToExprConverter convertor = new EvalNodeToExprConverter(scanNode.getTableName());
    convertor.visit(null, scanNode.getQual(), new Stack<>());

    Expr resultExpr = convertor.getResult();
    BinaryOperator equals = AlgebraicUtil.findTopExpr(resultExpr, OpType.Equals);
    assertNotNull(equals);

    ColumnReferenceExpr column = equals.getLeft();
    assertEquals("default.region", column.getQualifier());
    assertEquals("r_name", column.getName());

    LiteralValue literalValue = equals.getRight();
    assertEquals("EUROPE", literalValue.getValue());
    assertEquals(LiteralValue.LiteralType.String, literalValue.getValueType());
  }

  @Test
  public final void testBinaryOperator3() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[2]);

    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    LogicalNode node = plan.getRootBlock().getRoot();
    ScanNode scanNode = PlannerUtil.findTopNode(node, NodeType.SCAN);

    EvalNodeToExprConverter convertor = new EvalNodeToExprConverter(scanNode.getTableName());
    convertor.visit(null, scanNode.getQual(), new Stack<>());

    Expr resultExpr = convertor.getResult();

    BinaryOperator greaterThanOrEquals = AlgebraicUtil.findTopExpr(resultExpr, OpType.GreaterThanOrEquals);
    assertNotNull(greaterThanOrEquals);

    ColumnReferenceExpr greaterThanOrEqualsLeft = greaterThanOrEquals.getLeft();
    assertEquals("default.lineitem", greaterThanOrEqualsLeft.getQualifier());
    assertEquals("l_discount", greaterThanOrEqualsLeft.getName());

    LiteralValue greaterThanOrEqualsRight = greaterThanOrEquals.getRight();
    assertEquals("0.05", greaterThanOrEqualsRight.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Float, greaterThanOrEqualsRight.getValueType());

    BinaryOperator lessThanOrEquals = AlgebraicUtil.findTopExpr(resultExpr, OpType.LessThanOrEquals);
    assertNotNull(lessThanOrEquals);

    ColumnReferenceExpr lessThanOrEqualsLeft = lessThanOrEquals.getLeft();
    assertEquals("default.lineitem", lessThanOrEqualsLeft.getQualifier());
    assertEquals("l_discount", lessThanOrEqualsLeft.getName());

    LiteralValue lessThanOrEqualsRight = lessThanOrEquals.getRight();
    assertEquals("0.07", lessThanOrEqualsRight.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Float, lessThanOrEqualsRight.getValueType());

    BinaryOperator lessThan = AlgebraicUtil.findTopExpr(resultExpr, OpType.LessThan);
    assertNotNull(lessThan);

    ColumnReferenceExpr lessThanLeft = lessThan.getLeft();
    assertEquals("default.lineitem", lessThanLeft.getQualifier());
    assertEquals("l_quantity", lessThanLeft.getName());

    LiteralValue lessThanRight = lessThan.getRight();
    assertEquals("24.0", lessThanRight.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Float, lessThanRight.getValueType());

    BinaryOperator leftExpr = new BinaryOperator(OpType.And, greaterThanOrEquals, lessThanOrEquals);

    BinaryOperator topExpr = AlgebraicUtil.findTopExpr(resultExpr, OpType.Or);
    assertEquals(leftExpr, topExpr.getLeft());
    assertEquals(lessThan, topExpr.getRight());
  }

  @Test
  public final void testBetweenPredicate() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[3]);

    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    LogicalNode node = plan.getRootBlock().getRoot();
    ScanNode scanNode = PlannerUtil.findTopNode(node, NodeType.SCAN);

    EvalNodeToExprConverter convertor = new EvalNodeToExprConverter(scanNode.getTableName());
    convertor.visit(null, scanNode.getQual(), new Stack<>());

    Expr resultExpr = convertor.getResult();

    BinaryOperator binaryOperator = AlgebraicUtil.findTopExpr(resultExpr, OpType.LessThan);
    assertNotNull(binaryOperator);
    ColumnReferenceExpr column = binaryOperator.getLeft();
    assertEquals("default.lineitem", column.getQualifier());
    assertEquals("l_orderkey", column.getName());

    LiteralValue literalValue = binaryOperator.getRight();
    assertEquals("24", literalValue.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Integer, literalValue.getValueType());

    BetweenPredicate between = AlgebraicUtil.findTopExpr(resultExpr, OpType.Between);
    assertFalse(between.isNot());
    assertFalse(between.isSymmetric());

    ColumnReferenceExpr predicand = (ColumnReferenceExpr)between.predicand();
    assertEquals("default.lineitem", predicand.getQualifier());
    assertEquals("l_discount", predicand.getName());

    BinaryOperator begin = (BinaryOperator)between.begin();
    assertEquals(OpType.Minus, begin.getType());
    LiteralValue left = begin.getLeft();
    assertEquals("0.06", left.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Float, left.getValueType());
    LiteralValue right = begin.getRight();
    assertEquals("0.01", right.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Float, right.getValueType());

    BinaryOperator end = (BinaryOperator)between.end();
    assertEquals(OpType.Plus, end.getType());
    left = end.getLeft();
    assertEquals("0.08", left.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Float, left.getValueType());
    right = end.getRight();
    assertEquals("0.02", right.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Float, right.getValueType());
  }

  @Test
  public final void testCaseWhenPredicate() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[4]);

    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    LogicalNode node = plan.getRootBlock().getRoot();
    ScanNode scanNode = PlannerUtil.findTopNode(node, NodeType.SCAN);

    EvalNodeToExprConverter convertor = new EvalNodeToExprConverter(scanNode.getTableName());
    convertor.visit(null, scanNode.getQual(), new Stack<>());

    Expr resultExpr = convertor.getResult();

    CaseWhenPredicate caseWhen = AlgebraicUtil.findTopExpr(resultExpr, OpType.CaseWhen);
    assertNotNull(caseWhen);

    CaseWhenPredicate.WhenExpr[] whenExprs = new CaseWhenPredicate.WhenExpr[1];
    caseWhen.getWhens().toArray(whenExprs);

    BinaryOperator condition = (BinaryOperator) whenExprs[0].getCondition();
    assertEquals(OpType.GreaterThan, condition.getType());

    ColumnReferenceExpr conditionLeft = condition.getLeft();
    assertEquals("default.lineitem", conditionLeft.getQualifier());
    assertEquals("l_discount", conditionLeft.getName());

    LiteralValue conditionRight = condition.getRight();
    assertEquals("0.0", conditionRight.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Float, conditionRight.getValueType());

    BinaryOperator result = (BinaryOperator) whenExprs[0].getResult();
    assertEquals(OpType.Divide, result.getType());
    ColumnReferenceExpr resultLeft = result.getLeft();
    assertEquals("default.lineitem", resultLeft.getQualifier());
    assertEquals("l_discount", resultLeft.getName());

    ColumnReferenceExpr resultRight = result.getRight();
    assertEquals("default.lineitem", resultRight.getQualifier());
    assertEquals("l_tax", resultRight.getName());

    BinaryOperator greaterThan = AlgebraicUtil.findMostBottomExpr(resultExpr, OpType.GreaterThan);
    assertNotNull(greaterThan);

    assertEquals(greaterThan.getLeft(), caseWhen);

    LiteralValue binaryOperatorRight = greaterThan.getRight();
    assertEquals("1.2", binaryOperatorRight.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Float, conditionRight.getValueType());
  }

  @Test
  public final void testThreeFilters() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[5]);

    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    LogicalNode node = plan.getRootBlock().getRoot();
    ScanNode scanNode = PlannerUtil.findTopNode(node, NodeType.SCAN);

    EvalNodeToExprConverter convertor = new EvalNodeToExprConverter(scanNode.getTableName());
    convertor.visit(null, scanNode.getQual(), new Stack<>());

    Expr resultExpr = convertor.getResult();

    BetweenPredicate between = AlgebraicUtil.findTopExpr(resultExpr, OpType.Between);
    assertFalse(between.isNot());
    assertFalse(between.isSymmetric());

    ColumnReferenceExpr predicand = (ColumnReferenceExpr)between.predicand();
    assertEquals("default.part", predicand.getQualifier());
    assertEquals("p_size", predicand.getName());

    LiteralValue begin = (LiteralValue)between.begin();
    assertEquals("1", begin.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Integer, begin.getValueType());

    LiteralValue end = (LiteralValue)between.end();
    assertEquals("10", end.getValue());
    assertEquals(LiteralValue.LiteralType.Unsigned_Integer, end.getValueType());

    BinaryOperator equals = AlgebraicUtil.findTopExpr(resultExpr, OpType.Equals);
    assertNotNull(equals);

    ColumnReferenceExpr equalsLeft = equals.getLeft();
    assertEquals("default.part", equalsLeft.getQualifier());
    assertEquals("p_brand", equalsLeft.getName());

    LiteralValue equalsRight = equals.getRight();
    assertEquals("Brand#23", equalsRight.getValue());
    assertEquals(LiteralValue.LiteralType.String, equalsRight.getValueType());

    InPredicate inPredicate = AlgebraicUtil.findTopExpr(resultExpr, OpType.InPredicate);
    assertNotNull(inPredicate);

    ValueListExpr valueList = (ValueListExpr)inPredicate.getInValue();
    assertEquals(4, valueList.getValues().length);
    for(int i = 0; i < valueList.getValues().length; i++) {
      LiteralValue literalValue = (LiteralValue) valueList.getValues()[i];

      if (i == 0) {
        assertEquals("MED BAG", literalValue.getValue());
      } else if (i == 1) {
        assertEquals("MED BOX", literalValue.getValue());
      } else if (i == 2) {
        assertEquals("MED PKG", literalValue.getValue());
      } else {
        assertEquals("MED PACK", literalValue.getValue());
      }
    }
  }
}
