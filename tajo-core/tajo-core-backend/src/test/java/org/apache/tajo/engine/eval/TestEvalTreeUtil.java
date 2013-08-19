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

package org.apache.tajo.engine.eval;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.TestEvalTree.TestSum;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.EvalExprNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.master.TajoMaster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestEvalTreeUtil {
  static TajoTestingCluster util;
  static CatalogService catalog = null;
  static EvalNode expr1;
  static EvalNode expr2;
  static EvalNode expr3;
  static SQLAnalyzer analyzer;
  static LogicalPlanner planner;


  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.registerFunction(funcDesc);
    }

    Schema schema = new Schema();
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    schema.addColumn("score", TajoDataTypes.Type.INT4);
    schema.addColumn("age", TajoDataTypes.Type.INT4);

    TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta, new Path("file:///"));
    catalog.addTable(desc);

    FunctionDesc funcMeta = new FunctionDesc("sum", TestSum.class,
        FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(TajoDataTypes.Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(TajoDataTypes.Type.INT4, TajoDataTypes.Type.INT4));
    catalog.registerFunction(funcMeta);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);

    expr1 = getRootSelection(TestEvalTree.QUERIES[0]);
    expr2 = getRootSelection(TestEvalTree.QUERIES[1]);
    expr3 = getRootSelection(TestEvalTree.QUERIES[2]);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  public static Target [] getRawTargets(String query) {
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(expr);
    if (plan.getRootBlock().getRoot().getType() == NodeType.EXPRS) {
      return ((EvalExprNode)plan.getRootBlock().getRoot()).getExprs();
    } else {
      return plan.getRootBlock().getTargetListManager().getUnEvaluatedTargets();
    }
  }

  public static EvalNode getRootSelection(String query) {
    Expr block = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(block);
    return plan.getRootBlock().getSelectionNode().getQual();
  }

  @Test
  public final void testChangeColumnRef() throws CloneNotSupportedException {
    EvalNode copy = (EvalNode)expr1.clone();
    EvalTreeUtil.changeColumnRef(copy, "people.score", "newscore");
    Set<Column> set = EvalTreeUtil.findDistinctRefColumns(copy);
    assertEquals(1, set.size());
    assertTrue(set.contains(new Column("newscore", TajoDataTypes.Type.INT4)));
    
    copy = (EvalNode)expr2.clone();
    EvalTreeUtil.changeColumnRef(copy, "people.age", "sum_age");
    set = EvalTreeUtil.findDistinctRefColumns(copy);
    assertEquals(2, set.size());
    assertTrue(set.contains(new Column("people.score", TajoDataTypes.Type.INT4)));
    assertTrue(set.contains(new Column("sum_age", TajoDataTypes.Type.INT4)));
    
    copy = (EvalNode)expr3.clone();
    EvalTreeUtil.changeColumnRef(copy, "people.age", "sum_age");
    set = EvalTreeUtil.findDistinctRefColumns(copy);
    assertEquals(2, set.size());
    assertTrue(set.contains(new Column("people.score", TajoDataTypes.Type.INT4)));
    assertTrue(set.contains(new Column("sum_age", TajoDataTypes.Type.INT4)));
  }

  @Test
  public final void testFindAllRefColumns() {    
    Set<Column> set = EvalTreeUtil.findDistinctRefColumns(expr1);
    assertEquals(1, set.size());
    assertTrue(set.contains(new Column("people.score", TajoDataTypes.Type.INT4)));
    
    set = EvalTreeUtil.findDistinctRefColumns(expr2);
    assertEquals(2, set.size());
    assertTrue(set.contains(new Column("people.score", TajoDataTypes.Type.INT4)));
    assertTrue(set.contains(new Column("people.age", TajoDataTypes.Type.INT4)));
    
    set = EvalTreeUtil.findDistinctRefColumns(expr3);
    assertEquals(2, set.size());
    assertTrue(set.contains(new Column("people.score", TajoDataTypes.Type.INT4)));
    assertTrue(set.contains(new Column("people.age", TajoDataTypes.Type.INT4)));
  }
  
  public static final String [] QUERIES = {
    "select 3 + 4 as plus, (3.5 * 2) as mul", // 0
    "select (score + 3) < 4, age > 5 from people", // 1
    "select score from people where score > 7", // 2
    "select score from people where (10 * 2) * (score + 2) > 20 + 30 + 10", // 3
    "select score from people where 10 * 2 > score * 10", // 4
    "select score from people where score < 10 and 4 < score", // 5
    "select score from people where score < 10 and 4 < score and age > 5", // 6
  };
  
  @Test
  public final void testGetSchemaFromTargets() throws InternalException {
    Target [] targets = getRawTargets(QUERIES[0]);
    Schema schema = EvalTreeUtil.getSchemaByTargets(null, targets);
    Column col1 = schema.getColumn(0);
    Column col2 = schema.getColumn(1);
    assertEquals("plus", col1.getColumnName());
    assertEquals(TajoDataTypes.Type.INT4, col1.getDataType().getType());
    assertEquals("mul", col2.getColumnName());
    assertEquals(TajoDataTypes.Type.FLOAT8, col2.getDataType().getType());
  }
  
  @Test
  public final void testGetContainExprs() throws CloneNotSupportedException {
    Expr expr = analyzer.parse(QUERIES[1]);
    LogicalPlan plan = planner.createPlan(expr);
    Target [] targets = plan.getRootBlock().getTargetListManager().getUnEvaluatedTargets();
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    Collection<EvalNode> exprs =
        EvalTreeUtil.getContainExpr(targets[0].getEvalTree(), col1);
    EvalNode node = exprs.iterator().next();
    assertEquals(EvalType.LTH, node.getType());
    assertEquals(EvalType.PLUS, node.getLeftExpr().getType());
    assertEquals(new ConstEval(DatumFactory.createInt4(4)), node.getRightExpr());
    
    Column col2 = new Column("people.age", TajoDataTypes.Type.INT4);
    exprs = EvalTreeUtil.getContainExpr(targets[1].getEvalTree(), col2);
    node = exprs.iterator().next();
    assertEquals(EvalType.GTH, node.getType());
    assertEquals("people.age", node.getLeftExpr().getName());
    assertEquals(new ConstEval(DatumFactory.createInt4(5)), node.getRightExpr());
  }
  
  @Test
  public final void testGetCNF() {
    // "select score from people where score < 10 and 4 < score "
    EvalNode node = getRootSelection(QUERIES[5]);
    EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(node);
    
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    
    assertEquals(2, cnf.length);
    EvalNode first = cnf[0];
    EvalNode second = cnf[1];
    
    FieldEval field = (FieldEval) first.getLeftExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(EvalType.LTH, first.getType());
    EvalContext firstRCtx = first.getRightExpr().newContext();
    first.getRightExpr().eval(firstRCtx, null,  null);
    assertEquals(10, first.getRightExpr().terminate(firstRCtx).asInt4());
    
    field = (FieldEval) second.getRightExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(EvalType.LTH, second.getType());
    EvalContext secondLCtx = second.getLeftExpr().newContext();
    second.getLeftExpr().eval(secondLCtx, null,  null);
    assertEquals(4, second.getLeftExpr().terminate(secondLCtx).asInt4());
  }
  
  @Test
  public final void testTransformCNF2Singleton() {
    // "select score from people where score < 10 and 4 < score "
    EvalNode node = getRootSelection(QUERIES[6]);
    EvalNode [] cnf1 = EvalTreeUtil.getConjNormalForm(node);
    assertEquals(3, cnf1.length);
    
    EvalNode conj = EvalTreeUtil.transformCNF2Singleton(cnf1);
    EvalNode [] cnf2 = EvalTreeUtil.getConjNormalForm(conj);
    
    Set<EvalNode> set1 = Sets.newHashSet(cnf1);
    Set<EvalNode> set2 = Sets.newHashSet(cnf2);
    assertEquals(set1, set2);
  }
  
  @Test
  public final void testSimplify() {
    Target [] targets = getRawTargets(QUERIES[0]);
    EvalNode node = AlgebraicUtil.simplify(targets[0].getEvalTree());
    EvalContext nodeCtx = node.newContext();
    assertEquals(EvalType.CONST, node.getType());
    node.eval(nodeCtx, null, null);
    assertEquals(7, node.terminate(nodeCtx).asInt4());
    node = AlgebraicUtil.simplify(targets[1].getEvalTree());
    assertEquals(EvalType.CONST, node.getType());
    nodeCtx = node.newContext();
    node.eval(nodeCtx, null, null);
    assertTrue(7.0d == node.terminate(nodeCtx).asFloat8());

    Expr expr = analyzer.parse(QUERIES[1]);
    LogicalPlan plan = planner.createPlan(expr);
    targets = plan.getRootBlock().getTargetListManager().getUnEvaluatedTargets();
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    Collection<EvalNode> exprs =
        EvalTreeUtil.getContainExpr(targets[0].getEvalTree(), col1);
    node = exprs.iterator().next();
  }
  
  @Test
  public final void testConatainSingleVar() {
    EvalNode node = getRootSelection(QUERIES[2]);
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
    node = getRootSelection(QUERIES[3]);
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
  }
  
  @Test
  public final void testTranspose() {
    //EvalNode node = getRootSelection(QUERIES[2]);
    //assertEquals(true, AlgebraicUtil.containSingleVar(node));
    
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    EvalNode node = getRootSelection(QUERIES[3]);
    // we expect that score < 3
    EvalNode transposed = AlgebraicUtil.transpose(node, col1);
    assertEquals(EvalType.GTH, transposed.getType());
    FieldEval field = (FieldEval) transposed.getLeftExpr(); 
    assertEquals(col1, field.getColumnRef());
    EvalContext evalCtx = transposed.getRightExpr().newContext();
    transposed.getRightExpr().eval(evalCtx, null, null);
    assertEquals(1, transposed.getRightExpr().terminate(evalCtx).asInt4());

    node = getRootSelection(QUERIES[4]);
    // we expect that score < 3
    transposed = AlgebraicUtil.transpose(node, col1);
    assertEquals(EvalType.LTH, transposed.getType());
    field = (FieldEval) transposed.getLeftExpr(); 
    assertEquals(col1, field.getColumnRef());
    evalCtx = transposed.getRightExpr().newContext();
    transposed.getRightExpr().eval(evalCtx, null, null);
    assertEquals(2, transposed.getRightExpr().terminate(evalCtx).asInt4());
  }

  @Test
  public final void testFindDistinctAggFunctions() {
    String query = "select sum(score) + max(age) from people";
    Target [] targets = getRawTargets(query);
    List<AggFuncCallEval> list = EvalTreeUtil.
        findDistinctAggFunction(targets[0].getEvalTree());
    assertEquals(2, list.size());
    Set<String> result = Sets.newHashSet("max", "sum");
    for (AggFuncCallEval eval : list) {
      assertTrue(result.contains(eval.getName()));
    }
  }
}