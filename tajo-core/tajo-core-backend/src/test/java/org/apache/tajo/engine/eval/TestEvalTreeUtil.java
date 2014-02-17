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
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.Selection;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.apache.tajo.common.TajoDataTypes.Type.INT4;
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
  public static class TestSum extends GeneralFunction {
    private Integer x;
    private Integer y;

    public TestSum() {
      super(new Column[] { new Column("arg1", INT4),
          new Column("arg2", INT4) });
    }

    @Override
    public Datum eval(Tuple params) {
      x =  params.get(0).asInt4();
      y =  params.get(1).asInt4();
      return DatumFactory.createInt4(x + y);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }

    Schema schema = new Schema();
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    schema.addColumn("score", TajoDataTypes.Type.INT4);
    schema.addColumn("age", TajoDataTypes.Type.INT4);

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
    TableDesc desc = new TableDesc("people", schema, meta, CommonTestingUtil.getTestDir());
    catalog.addTable(desc);

    FunctionDesc funcMeta = new FunctionDesc("test_sum", TestSum.class,
        FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(TajoDataTypes.Type.INT4, TajoDataTypes.Type.INT4));
    catalog.createFunction(funcMeta);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);

    String[] QUERIES = {
        "select name, score, age from people where score > 30", // 0
        "select name, score, age from people where score * age", // 1
        "select name, score, age from people where test_sum(score * age, 50)", // 2
    };

    expr1 = getRootSelection(QUERIES[0]);
    expr2 = getRootSelection(QUERIES[1]);
    expr3 = getRootSelection(QUERIES[2]);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  public static Target [] getRawTargets(String query) {
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = null;
    try {
      plan = planner.createPlan(expr);
    } catch (PlanningException e) {
      e.printStackTrace();
    }

    return plan.getRootBlock().getRawTargets();
  }

  public static EvalNode getRootSelection(String query) throws PlanningException {
    Expr block = analyzer.parse(query);
    LogicalPlan plan = null;
    try {
      plan = planner.createPlan(block);
    } catch (PlanningException e) {
      e.printStackTrace();
    }

    Selection selection = plan.getRootBlock().getSingletonExpr(OpType.Filter);
    return planner.getExprAnnotator().createEvalNode(plan, plan.getRootBlock(), selection.getQual());
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
    "select score from people where (score > 1 and score < 3) or (7 < score and score < 10)", // 7
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
  public final void testGetContainExprs() throws CloneNotSupportedException, PlanningException {
    Expr expr = analyzer.parse(QUERIES[1]);
    LogicalPlan plan = planner.createPlan(expr, true);
    Target [] targets = plan.getRootBlock().getRawTargets();
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
  public final void testGetCNF() throws PlanningException {
    // "select score from people where score < 10 and 4 < score "
    EvalNode node = getRootSelection(QUERIES[5]);
    EvalNode [] cnf = AlgebraicUtil.toConjunctiveNormalFormArray(node);
    
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    
    assertEquals(2, cnf.length);
    EvalNode first = cnf[0];
    EvalNode second = cnf[1];
    
    FieldEval field = first.getLeftExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(EvalType.LTH, first.getType());
    assertEquals(10, first.getRightExpr().eval(null,  null).asInt4());
    
    field = second.getRightExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(EvalType.LTH, second.getType());
    assertEquals(4, second.getLeftExpr().eval(null,  null).asInt4());
  }
  
  @Test
  public final void testTransformCNF2Singleton() throws PlanningException {
    // "select score from people where score < 10 and 4 < score "
    EvalNode node = getRootSelection(QUERIES[6]);
    EvalNode [] cnf1 = AlgebraicUtil.toConjunctiveNormalFormArray(node);
    assertEquals(3, cnf1.length);
    
    EvalNode conj = AlgebraicUtil.createSingletonExprFromCNF(cnf1);
    EvalNode [] cnf2 = AlgebraicUtil.toConjunctiveNormalFormArray(conj);
    
    Set<EvalNode> set1 = Sets.newHashSet(cnf1);
    Set<EvalNode> set2 = Sets.newHashSet(cnf2);
    assertEquals(set1, set2);
  }

  @Test
  public final void testGetDNF() throws PlanningException {
    // "select score from people where score > 1 and score < 3 or score > 7 and score < 10", // 7
    EvalNode node = getRootSelection(QUERIES[7]);
    EvalNode [] cnf = AlgebraicUtil.toDisjunctiveNormalFormArray(node);
    assertEquals(2, cnf.length);

    assertEquals("people.score (INT4) > 1 AND people.score (INT4) < 3", cnf[0].toString());
    assertEquals("7 < people.score (INT4) AND people.score (INT4) < 10", cnf[1].toString());
  }
  
  @Test
  public final void testSimplify() throws PlanningException {
    Target [] targets = getRawTargets(QUERIES[0]);
    EvalNode node = AlgebraicUtil.eliminateConstantExprs(targets[0].getEvalTree());
    assertEquals(EvalType.CONST, node.getType());
    assertEquals(7, node.eval(null, null).asInt4());
    node = AlgebraicUtil.eliminateConstantExprs(targets[1].getEvalTree());
    assertEquals(EvalType.CONST, node.getType());
    assertTrue(7.0d == node.eval(null, null).asFloat8());

    Expr expr = analyzer.parse(QUERIES[1]);
    LogicalPlan plan = planner.createPlan(expr, true);
    targets = plan.getRootBlock().getRawTargets();
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    Collection<EvalNode> exprs =
        EvalTreeUtil.getContainExpr(targets[0].getEvalTree(), col1);
    node = exprs.iterator().next();
  }
  
  @Test
  public final void testConatainSingleVar() throws PlanningException {
    EvalNode node = getRootSelection(QUERIES[2]);
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
    node = getRootSelection(QUERIES[3]);
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
  }
  
  @Test
  public final void testTranspose() throws PlanningException {
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    EvalNode node = getRootSelection(QUERIES[3]);
    // we expect that score < 3
    EvalNode transposed = AlgebraicUtil.transpose(node, col1);
    assertEquals(EvalType.GTH, transposed.getType());
    FieldEval field = transposed.getLeftExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(1, transposed.getRightExpr().eval(null, null).asInt4());

    node = getRootSelection(QUERIES[4]);
    // we expect that score < 3
    transposed = AlgebraicUtil.transpose(node, col1);
    assertEquals(EvalType.LTH, transposed.getType());
    field = transposed.getLeftExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(2, transposed.getRightExpr().eval(null, null).asInt4());
  }

  @Test
  public final void testFindDistinctAggFunctions() throws PlanningException {
    String query = "select sum(score) + max(age) from people";
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(expr);
    GroupbyNode groupByNode = plan.getRootBlock().getNode(NodeType.GROUP_BY);
    EvalNode [] aggEvals = groupByNode.getAggFunctions();

    List<AggregationFunctionCallEval> list = new ArrayList<AggregationFunctionCallEval>();
    for (int i = 0; i < aggEvals.length; i++) {
      list.addAll(EvalTreeUtil.findDistinctAggFunction(aggEvals[i]));
    }
    assertEquals(2, list.size());

    Set<String> result = Sets.newHashSet("max", "sum");
    for (AggregationFunctionCallEval eval : list) {
      assertTrue(result.contains(eval.getName()));
    }
  }
}