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

package tajo.engine.eval;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.TajoTestingCluster;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.common.TajoDataTypes;
import tajo.datum.DatumFactory;
import tajo.engine.eval.EvalNode.Type;
import tajo.engine.eval.TestEvalTree.TestSum;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.parser.QueryBlock;
import tajo.engine.parser.QueryBlock.Target;
import tajo.engine.planner.LogicalPlanner;
import tajo.exception.InternalException;
import tajo.master.TajoMaster;

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
  static QueryAnalyzer analyzer;
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

    analyzer = new QueryAnalyzer(catalog);
    planner = new LogicalPlanner(catalog);
    
    QueryBlock block;

    block = (QueryBlock) analyzer.parse(TestEvalTree.QUERIES[0]).getParseTree();
    expr1 = block.getWhereCondition();

    block = (QueryBlock) analyzer.parse(TestEvalTree.QUERIES[1]).getParseTree();
    expr2 = block.getWhereCondition();
    
    block = (QueryBlock) analyzer.parse(TestEvalTree.QUERIES[2]).getParseTree();
    expr3 = block.getWhereCondition();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
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
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[0]).getParseTree();
    Schema schema = 
        EvalTreeUtil.getSchemaByTargets(null, block.getTargetList());
    Column col1 = schema.getColumn(0);
    Column col2 = schema.getColumn(1);
    assertEquals("plus", col1.getColumnName());
    assertEquals(TajoDataTypes.Type.INT4, col1.getDataType().getType());
    assertEquals("mul", col2.getColumnName());
    assertEquals(TajoDataTypes.Type.FLOAT8, col2.getDataType().getType());
  }
  
  @Test
  public final void testGetContainExprs() throws CloneNotSupportedException {
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[1]).getParseTree();
    Target [] targets = block.getTargetList();
    
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    Collection<EvalNode> exprs =
        EvalTreeUtil.getContainExpr(targets[0].getEvalTree(), col1);
    EvalNode node = exprs.iterator().next();
    assertEquals(Type.LTH, node.getType());
    assertEquals(Type.PLUS, node.getLeftExpr().getType());
    assertEquals(new ConstEval(DatumFactory.createInt4(4)), node.getRightExpr());
    
    Column col2 = new Column("people.age", TajoDataTypes.Type.INT4);
    exprs = EvalTreeUtil.getContainExpr(targets[1].getEvalTree(), col2);
    node = exprs.iterator().next();
    assertEquals(Type.GTH, node.getType());
    assertEquals("people.age", node.getLeftExpr().getName());
    assertEquals(new ConstEval(DatumFactory.createInt4(5)), node.getRightExpr());
  }
  
  @Test
  public final void testGetCNF() {
    // "select score from people where score < 10 and 4 < score "
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[5]).getParseTree();
    EvalNode node = block.getWhereCondition();
    EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(node);
    
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    
    assertEquals(2, cnf.length);
    EvalNode first = cnf[0];
    EvalNode second = cnf[1];
    
    FieldEval field = (FieldEval) first.getLeftExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(Type.LTH, first.getType());
    EvalContext firstRCtx = first.getRightExpr().newContext();
    first.getRightExpr().eval(firstRCtx, null,  null);
    assertEquals(10, first.getRightExpr().terminate(firstRCtx).asInt4());
    
    field = (FieldEval) second.getRightExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(Type.LTH, second.getType());
    EvalContext secondLCtx = second.getLeftExpr().newContext();
    second.getLeftExpr().eval(secondLCtx, null,  null);
    assertEquals(4, second.getLeftExpr().terminate(secondLCtx).asInt4());
  }
  
  @Test
  public final void testTransformCNF2Singleton() {
    // "select score from people where score < 10 and 4 < score "
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[6]).getParseTree();
    EvalNode node = block.getWhereCondition();
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
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[0]).getParseTree();
    Target [] targets = block.getTargetList();
    EvalNode node = AlgebraicUtil.simplify(targets[0].getEvalTree());
    EvalContext nodeCtx = node.newContext();
    assertEquals(Type.CONST, node.getType());
    node.eval(nodeCtx, null, null);
    assertEquals(7, node.terminate(nodeCtx).asInt4());
    node = AlgebraicUtil.simplify(targets[1].getEvalTree());
    assertEquals(Type.CONST, node.getType());
    nodeCtx = node.newContext();
    node.eval(nodeCtx, null, null);
    assertTrue(7.0d == node.terminate(nodeCtx).asFloat8());

    block = (QueryBlock) analyzer.parse(QUERIES[1]).getParseTree();
    targets = block.getTargetList();
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    Collection<EvalNode> exprs =
        EvalTreeUtil.getContainExpr(targets[0].getEvalTree(), col1);
    node = exprs.iterator().next();
    System.out.println(AlgebraicUtil.simplify(node));
  }
  
  @Test
  public final void testConatainSingleVar() {
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[2]).getParseTree();
    EvalNode node = block.getWhereCondition();
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
    
    block = (QueryBlock) analyzer.parse(QUERIES[3]).getParseTree();
    node = block.getWhereCondition();
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
  }
  
  @Test
  public final void testTranspose() {
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[2]).getParseTree();
    EvalNode node = block.getWhereCondition();
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
    
    Column col1 = new Column("people.score", TajoDataTypes.Type.INT4);
    block = (QueryBlock) analyzer.parse(QUERIES[3]).getParseTree();
    node = block.getWhereCondition();    
    // we expect that score < 3
    EvalNode transposed = AlgebraicUtil.transpose(node, col1);
    assertEquals(Type.GTH, transposed.getType());
    FieldEval field = (FieldEval) transposed.getLeftExpr(); 
    assertEquals(col1, field.getColumnRef());
    EvalContext evalCtx = transposed.getRightExpr().newContext();
    transposed.getRightExpr().eval(evalCtx, null, null);
    assertEquals(1, transposed.getRightExpr().terminate(evalCtx).asInt4());

    block = (QueryBlock) analyzer.parse(QUERIES[4]).getParseTree();
    node = block.getWhereCondition();    
    // we expect that score < 3
    transposed = AlgebraicUtil.transpose(node, col1);
    assertEquals(Type.LTH, transposed.getType());
    field = (FieldEval) transposed.getLeftExpr(); 
    assertEquals(col1, field.getColumnRef());
    evalCtx = transposed.getRightExpr().newContext();
    transposed.getRightExpr().eval(evalCtx, null, null);
    assertEquals(2, transposed.getRightExpr().terminate(evalCtx).asInt4());
  }

  @Test
  public final void testFindDistinctAggFunctions() {

    QueryBlock block = (QueryBlock) analyzer.parse(
        "select sum(score) + max(age) from people").getParseTree();
    List<AggFuncCallEval> list = EvalTreeUtil.
        findDistinctAggFunction(block.getTargetList()[0].getEvalTree());
    assertEquals(2, list.size());
    Set<String> result = Sets.newHashSet("max", "sum");
    for (AggFuncCallEval eval : list) {
      assertTrue(result.contains(eval.getName()));
    }
  }
}