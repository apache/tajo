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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.function.GeneralFunction;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.EvalNode.Type;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.junit.Assert.*;

public class TestEvalTree {
  private static TajoTestingCluster util;
  private static CatalogService cat;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static Tuple [] tuples = new Tuple[3];
  
  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    cat = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      cat.registerFunction(funcDesc);
    }

    Schema schema = new Schema();
    schema.addColumn("name", TEXT);
    schema.addColumn("score", INT4);
    schema.addColumn("age", INT4);

    TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta, new Path("file:///"));
    cat.addTable(desc);

    FunctionDesc funcMeta = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(INT4),
        CatalogUtil.newDataTypesWithoutLen(INT4, INT4));
    cat.registerFunction(funcMeta);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(cat);
    
    tuples[0] = new VTuple(3);
    tuples[0].put(new Datum[] {
        DatumFactory.createText("aabc"),
        DatumFactory.createInt4(100),
        DatumFactory.createInt4(10)});
    tuples[1] = new VTuple(3);
    tuples[1].put(new Datum[] {
        DatumFactory.createText("aaba"),
        DatumFactory.createInt4(200),
        DatumFactory.createInt4(20)});
    tuples[2] = new VTuple(3);
    tuples[2].put(new Datum[] {
        DatumFactory.createText("kabc"),
        DatumFactory.createInt4(300),
        DatumFactory.createInt4(30)});
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

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

  static String[] QUERIES = {
      "select name, score, age from people where score > 30", // 0
      "select name, score, age from people where score * age", // 1
      "select name, score, age from people where sum(score * age, 50)", // 2
      "select 2+3", // 3
      "select sum(score) from people", // 4
      "select name from people where NOT (20 > 30)", // 5
  };

  public static Target[] getRawTargets(String query) {
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(expr);
    return plan.getRootBlock().getTargetListManager().getUnEvaluatedTargets();
  }

  public static EvalNode getRootSelection(String query) {
    Expr block = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(block);
    return plan.getRootBlock().getSelectionNode().getQual();
  }

  @Test
  public final void testFunctionEval() throws Exception {    
    Tuple tuple = new VTuple(3);
    tuple.put(
        new Datum[] {
          DatumFactory.createText("hyunsik"),
          DatumFactory.createInt4(500),
          DatumFactory.createInt4(30)});

    EvalNode expr;

    Schema peopleSchema = cat.getTableDesc("people").getMeta().getSchema();
    EvalContext evalCtx;
    expr = getRootSelection(QUERIES[0]);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, peopleSchema, tuple);
    assertEquals(true, expr.terminate(evalCtx).asBool());

    expr = getRootSelection(QUERIES[1]);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, peopleSchema, tuple);
    assertEquals(15000, expr.terminate(evalCtx).asInt4());
    assertCloneEqual(expr);

    expr = getRootSelection(QUERIES[2]);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, peopleSchema, tuple);
    assertEquals(15050, expr.terminate(evalCtx).asInt4());
    assertCloneEqual(expr);
    
    // Aggregation function test
    expr = getRawTargets(QUERIES[4])[0].getEvalTree();
    evalCtx = expr.newContext();
    
    final int tuplenum = 10;
    Tuple [] tuples = new Tuple[tuplenum];
    for (int i=0; i < tuplenum; i++) {
      tuples[i] = new VTuple(3);
      tuples[i].put(0, DatumFactory.createText("hyunsik"));
      tuples[i].put(1, DatumFactory.createInt4(i + 1));
      tuples[i].put(2, DatumFactory.createInt4(30));
    }
    
    int sum = 0;
    for (int i=0; i < tuplenum; i++) {
      expr.eval(evalCtx, peopleSchema, tuples[i]);
      sum = sum + (i+1);
      assertEquals(sum, expr.terminate(evalCtx).asInt4());
    }
  }
  
  
  @Test
  public void testTupleEval() throws CloneNotSupportedException {
    ConstEval e1 = new ConstEval(DatumFactory.createInt4(1));
    assertCloneEqual(e1);
    FieldEval e2 = new FieldEval("table1.score", CatalogUtil.newDataTypeWithoutLen(INT4)); // it indicates
    assertCloneEqual(e2);

    Schema schema1 = new Schema();
    schema1.addColumn("table1.id", INT4);
    schema1.addColumn("table1.score", INT4);
    
    BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
    EvalContext evalCtx = expr.newContext();
    assertCloneEqual(expr);
    VTuple tuple = new VTuple(2);
    tuple.put(0, DatumFactory.createInt4(1)); // put 0th field
    tuple.put(1, DatumFactory.createInt4(99)); // put 0th field

    // the result of evaluation must be 100.
    expr.eval(evalCtx, schema1, tuple);
    assertEquals(expr.terminate(evalCtx).asInt4(), 100);
  }

  public static class MockTrueEval extends EvalNode {

    public MockTrueEval() {
      super(Type.CONST);
    }

    @Override
    public String getName() {
      return this.getClass().getName();
    }

    @Override
    public Datum terminate(EvalContext ctx) {
      return DatumFactory.createBool(true);
    }

    @Override
    public boolean equals(Object obj) {
      return true;
    }

    @Override
    public EvalContext newContext() {
      return null;
    }

    @Override
    public DataType [] getValueType() {
      return CatalogUtil.newDataTypesWithoutLen(BOOLEAN);
    }

  }

  public static class MockFalseExpr extends EvalNode {

    public MockFalseExpr() {
      super(Type.CONST);
    }

    @Override
    public EvalContext newContext() {
      return null;
    }

    @Override
    public Datum terminate(EvalContext ctx) {
      return DatumFactory.createBool(false);
    }

    @Override
    public boolean equals(Object obj) {
      return true;
    }

    @Override
    public String getName() {
      return this.getClass().getName();
    }

    @Override
    public DataType [] getValueType() {
      return CatalogUtil.newDataTypesWithoutLen(BOOLEAN);
    }
  }

  @Test
  public void testAndTest() {
    MockTrueEval trueExpr = new MockTrueEval();
    MockFalseExpr falseExpr = new MockFalseExpr();

    BinaryEval andExpr = new BinaryEval(Type.AND, trueExpr, trueExpr);
    EvalContext evalCtx = andExpr.newContext();
    andExpr.eval(evalCtx, null, null);
    assertTrue(andExpr.terminate(evalCtx).asBool());

    andExpr = new BinaryEval(Type.AND, falseExpr, trueExpr);
    evalCtx = andExpr.newContext();
    andExpr.eval(evalCtx, null, null);
    assertFalse(andExpr.terminate(evalCtx).asBool());

    andExpr = new BinaryEval(Type.AND, trueExpr, falseExpr);
    evalCtx= andExpr.newContext();
    andExpr.eval(evalCtx, null, null);
    assertFalse(andExpr.terminate(evalCtx).asBool());

    andExpr = new BinaryEval(Type.AND, falseExpr, falseExpr);
    evalCtx= andExpr.newContext();
    andExpr.eval(evalCtx, null, null);
    assertFalse(andExpr.terminate(evalCtx).asBool());
  }

  @Test
  public void testOrTest() {
    MockTrueEval trueExpr = new MockTrueEval();
    MockFalseExpr falseExpr = new MockFalseExpr();

    BinaryEval orExpr = new BinaryEval(Type.OR, trueExpr, trueExpr);
    EvalContext evalCtx= orExpr.newContext();
    orExpr.eval(evalCtx, null, null);
    assertTrue(orExpr.terminate(evalCtx).asBool());

    orExpr = new BinaryEval(Type.OR, falseExpr, trueExpr);
    evalCtx= orExpr.newContext();
    orExpr.eval(evalCtx, null, null);
    assertTrue(orExpr.terminate(evalCtx).asBool());

    orExpr = new BinaryEval(Type.OR, trueExpr, falseExpr);
    evalCtx= orExpr.newContext();
    orExpr.eval(evalCtx, null, null);
    assertTrue(orExpr.terminate(evalCtx).asBool());

    orExpr = new BinaryEval(Type.OR, falseExpr, falseExpr);
    evalCtx = orExpr.newContext();
    orExpr.eval(evalCtx, null, null);
    assertFalse(orExpr.terminate(evalCtx).asBool());
  }

  @Test
  public final void testCompOperator() {
    ConstEval e1;
    ConstEval e2;
    BinaryEval expr;

    // Constant
    e1 = new ConstEval(DatumFactory.createInt4(9));
    e2 = new ConstEval(DatumFactory.createInt4(34));
    expr = new BinaryEval(Type.LTH, e1, e2);
    EvalContext evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.LEQ, e1, e2);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.LTH, e2, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.LEQ, e2, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());

    expr = new BinaryEval(Type.GTH, e2, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.GEQ, e2, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.GTH, e1, e2);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.GEQ, e1, e2);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());

    BinaryEval plus = new BinaryEval(Type.PLUS, e1, e2);
    expr = new BinaryEval(Type.LTH, e1, plus);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.LEQ, e1, plus);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.LTH, plus, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.LEQ, plus, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());

    expr = new BinaryEval(Type.GTH, plus, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.GEQ, plus, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.GTH, e1, plus);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());
    expr = new BinaryEval(Type.GEQ, e1, plus);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());
  }

  @Test
  public final void testArithmaticsOperator() 
      throws CloneNotSupportedException {
    ConstEval e1;
    ConstEval e2;

    // PLUS
    e1 = new ConstEval(DatumFactory.createInt4(9));
    e2 = new ConstEval(DatumFactory.createInt4(34));
    BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
    EvalContext evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertEquals(expr.terminate(evalCtx).asInt4(), 43);
    assertCloneEqual(expr);
    
    // MINUS
    e1 = new ConstEval(DatumFactory.createInt4(5));
    e2 = new ConstEval(DatumFactory.createInt4(2));
    expr = new BinaryEval(Type.MINUS, e1, e2);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertEquals(expr.terminate(evalCtx).asInt4(), 3);
    assertCloneEqual(expr);
    
    // MULTIPLY
    e1 = new ConstEval(DatumFactory.createInt4(5));
    e2 = new ConstEval(DatumFactory.createInt4(2));
    expr = new BinaryEval(Type.MULTIPLY, e1, e2);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertEquals(expr.terminate(evalCtx).asInt4(), 10);
    assertCloneEqual(expr);
    
    // DIVIDE
    e1 = new ConstEval(DatumFactory.createInt4(10));
    e2 = new ConstEval(DatumFactory.createInt4(5));
    expr = new BinaryEval(Type.DIVIDE, e1, e2);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertEquals(expr.terminate(evalCtx).asInt4(), 2);
    assertCloneEqual(expr);
  }

  @Test
  public final void testGetReturnType() {
    ConstEval e1;
    ConstEval e2;

    // PLUS
    e1 = new ConstEval(DatumFactory.createInt4(9));
    e2 = new ConstEval(DatumFactory.createInt4(34));
    BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
    assertEquals(CatalogUtil.newDataTypeWithoutLen(INT4), expr.getValueType()[0]);

    expr = new BinaryEval(Type.LTH, e1, e2);
    EvalContext evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    assertEquals(CatalogUtil.newDataTypeWithoutLen(BOOLEAN), expr.getValueType()[0]);

    e1 = new ConstEval(DatumFactory.createFloat8(9.3));
    e2 = new ConstEval(DatumFactory.createFloat8(34.2));
    expr = new BinaryEval(Type.PLUS, e1, e2);
    assertEquals(CatalogUtil.newDataTypeWithoutLen(FLOAT8), expr.getValueType()[0]);
  }
  
  @Test
  public final void testEquals() throws CloneNotSupportedException {
    ConstEval e1;
    ConstEval e2;

    // PLUS
    e1 = new ConstEval(DatumFactory.createInt4(34));
    e2 = new ConstEval(DatumFactory.createInt4(34));
    assertEquals(e1, e2);
    
    BinaryEval plus1 = new BinaryEval(Type.PLUS, e1, e2);
    BinaryEval plus2 = new BinaryEval(Type.PLUS, e2, e1);
    assertEquals(plus1, plus2);
    
    ConstEval e3 = new ConstEval(DatumFactory.createInt4(29));
    BinaryEval plus3 = new BinaryEval(Type.PLUS, e1, e3);
    assertFalse(plus1.equals(plus3));
    
    // LTH
    ConstEval e4 = new ConstEval(DatumFactory.createInt4(9));
    ConstEval e5 = new ConstEval(DatumFactory.createInt4(34));
    BinaryEval compExpr1 = new BinaryEval(Type.LTH, e4, e5);
    assertCloneEqual(compExpr1);
    
    ConstEval e6 = new ConstEval(DatumFactory.createInt4(9));
    ConstEval e7 = new ConstEval(DatumFactory.createInt4(34));
    BinaryEval compExpr2 = new BinaryEval(Type.LTH, e6, e7);
    assertCloneEqual(compExpr2);
    
    assertTrue(compExpr1.equals(compExpr2));
  }
  
  @Test
  public final void testJson() throws CloneNotSupportedException {
    ConstEval e1;
    ConstEval e2;

    // 29 > (34 + 5) + (5 + 34)
    e1 = new ConstEval(DatumFactory.createInt4(34));
    e2 = new ConstEval(DatumFactory.createInt4(5));
    assertCloneEqual(e1); 
    
    BinaryEval plus1 = new BinaryEval(Type.PLUS, e1, e2);
    assertCloneEqual(plus1);
    BinaryEval plus2 = new BinaryEval(Type.PLUS, e2, e1);
    assertCloneEqual(plus2);
    BinaryEval plus3 = new BinaryEval(Type.PLUS, plus2, plus1);
    assertCloneEqual(plus3);
    
    ConstEval e3 = new ConstEval(DatumFactory.createInt4(29));
    BinaryEval gth = new BinaryEval(Type.GTH, e3, plus3);
    assertCloneEqual(gth);
    
    String json = gth.toJson();
    EvalNode eval = CoreGsonHelper.fromJson(json, EvalNode.class);
    assertCloneEqual(eval);
    
    assertEquals(gth.getType(), eval.getType());
    assertEquals(e3.getType(), eval.getLeftExpr().getType());
    assertEquals(plus3.getType(), eval.getRightExpr().getType());
    assertEquals(plus3.getLeftExpr(), eval.getRightExpr().getLeftExpr());
    assertEquals(plus3.getRightExpr(), eval.getRightExpr().getRightExpr());
    assertEquals(plus2.getLeftExpr(), eval.getRightExpr().getLeftExpr().getLeftExpr());
    assertEquals(plus2.getRightExpr(), eval.getRightExpr().getLeftExpr().getRightExpr());
    assertEquals(plus1.getLeftExpr(), eval.getRightExpr().getRightExpr().getLeftExpr());
    assertEquals(plus1.getRightExpr(), eval.getRightExpr().getRightExpr().getRightExpr());
  }
  
  private void assertCloneEqual(EvalNode eval) throws CloneNotSupportedException {
    EvalNode copy = (EvalNode) eval.clone();
    assertEquals(eval, copy);
    assertFalse(eval == copy);
  }
  
  static String[] NOT = {
    "select name, score, age from people where not (score >= 200)", // 0"
  };
  
  @Test
  public final void testNot() throws CloneNotSupportedException {
    ConstEval e1;
    ConstEval e2;
    EvalNode expr;

    // Constant
    e1 = new ConstEval(DatumFactory.createInt4(9));
    e2 = new ConstEval(DatumFactory.createInt4(34));
    expr = new BinaryEval(Type.LTH, e1, e2);
    EvalContext evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    NotEval not = new NotEval(expr);
    evalCtx = not .newContext();
    not.eval(evalCtx, null, null);
    assertFalse(not.terminate(evalCtx).asBool());
    
    expr = new BinaryEval(Type.LEQ, e1, e2);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertTrue(expr.terminate(evalCtx).asBool());
    not = new NotEval(expr);
    evalCtx = not.newContext();
    not.eval(evalCtx, null, null);
    assertFalse(not.terminate(evalCtx).asBool());
    
    expr = new BinaryEval(Type.LTH, e2, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());
    not = new NotEval(expr);
    evalCtx = not.newContext();
    not.eval(evalCtx, null, null);
    assertTrue(not.terminate(evalCtx).asBool());
    
    expr = new BinaryEval(Type.LEQ, e2, e1);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, null, null);
    assertFalse(expr.terminate(evalCtx).asBool());
    not = new NotEval(expr);
    evalCtx = not.newContext();
    not.eval(evalCtx, null, null);
    assertTrue(not.terminate(evalCtx).asBool());
    
    // Evaluation Test
    Schema peopleSchema = cat.getTableDesc("people").getMeta().getSchema();
    expr = getRootSelection(NOT[0]);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, peopleSchema, tuples[0]);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr.eval(evalCtx, peopleSchema, tuples[1]);
    assertFalse(expr.terminate(evalCtx).asBool());
    expr.eval(evalCtx, peopleSchema, tuples[2]);
    assertFalse(expr.terminate(evalCtx).asBool());
  }
  
  static String[] LIKE = {
    "select name, score, age from people where name like '%bc'", // 0"
    "select name, score, age from people where name like 'aa%'", // 1"
    "select name, score, age from people where name not like '%bc'", // 2"
  };
  
  @Test
  public final void testLike() {
    EvalNode expr;

    Schema peopleSchema = cat.getTableDesc("people").getMeta().getSchema();
    expr = getRootSelection(LIKE[0]);
    EvalContext evalCtx = expr.newContext();
    expr.eval(evalCtx, peopleSchema, tuples[0]);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr.eval(evalCtx, peopleSchema, tuples[1]);
    assertFalse(expr.terminate(evalCtx).asBool());
    expr.eval(evalCtx, peopleSchema, tuples[2]);
    assertTrue(expr.terminate(evalCtx).asBool());
    
    // prefix
    expr = getRootSelection(LIKE[1]);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, peopleSchema, tuples[0]);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr.eval(evalCtx, peopleSchema, tuples[1]);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr.eval(evalCtx, peopleSchema, tuples[2]);
    assertFalse(expr.terminate(evalCtx).asBool());

    // Not Test
    expr = getRootSelection(LIKE[2]);
    evalCtx = expr.newContext();
    expr.eval(evalCtx, peopleSchema, tuples[0]);
    assertFalse(expr.terminate(evalCtx).asBool());
    expr.eval(evalCtx, peopleSchema, tuples[1]);
    assertTrue(expr.terminate(evalCtx).asBool());
    expr.eval(evalCtx, peopleSchema, tuples[2]);
    assertFalse(expr.terminate(evalCtx).asBool());
  }

  static String[] IS_NULL = {
      "select name, score, age from people where name is null", // 0"
      "select name, score, age from people where name is not null", // 1"
  };

  @Test
  public void testIsNullEval() {
    EvalNode expr;

    expr = getRootSelection(IS_NULL[0]);

    assertIsNull(expr);

    expr = getRootSelection(IS_NULL[1]);

    IsNullEval nullEval = (IsNullEval) expr;
    assertTrue(nullEval.isNot());
    assertIsNull(expr);
  }

  private void assertIsNull(EvalNode isNullEval) {
    assertEquals(Type.IS, isNullEval.getType());
    assertEquals(Type.FIELD, isNullEval.getLeftExpr().getType());
    FieldEval left = (FieldEval) isNullEval.getLeftExpr();
    assertEquals("name", left.getColumnName());
    assertEquals(Type.CONST, isNullEval.getRightExpr().getType());
    ConstEval constEval = (ConstEval) isNullEval.getRightExpr();
    assertEquals(DatumFactory.createNullDatum(), constEval.getValue());
  }
}
