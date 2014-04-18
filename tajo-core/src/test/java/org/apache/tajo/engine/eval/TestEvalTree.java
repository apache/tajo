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

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.Test;

import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.junit.Assert.*;

public class TestEvalTree extends ExprTestBase{
  @Test
  public void testTupleEval() throws CloneNotSupportedException {
    ConstEval e1 = new ConstEval(DatumFactory.createInt4(1));
    assertCloneEqual(e1);
    FieldEval e2 = new FieldEval("table1.score", CatalogUtil.newSimpleDataType(INT4)); // it indicates
    assertCloneEqual(e2);

    Schema schema1 = new Schema();
    schema1.addColumn("table1.id", INT4);
    schema1.addColumn("table1.score", INT4);
    
    BinaryEval expr = new BinaryEval(EvalType.PLUS, e1, e2);
    assertCloneEqual(expr);
    VTuple tuple = new VTuple(2);
    tuple.put(0, DatumFactory.createInt4(1)); // put 0th field
    tuple.put(1, DatumFactory.createInt4(99)); // put 0th field

    // the result of evaluation must be 100.
    assertEquals(expr.eval(schema1, tuple).asInt4(), 100);
  }

  public static class MockTrueEval extends EvalNode {

    public MockTrueEval() {
      super(EvalType.CONST);
    }

    @Override
    public String getName() {
      return this.getClass().getName();
    }

    @Override
    public Datum eval(Schema schema, Tuple tuple) {
      return DatumFactory.createBool(true);
    }

    @Override
    public boolean equals(Object obj) {
      return true;
    }

    @Override
    public DataType getValueType() {
      return CatalogUtil.newSimpleDataType(BOOLEAN);
    }

  }

  public static class MockFalseExpr extends EvalNode {

    public MockFalseExpr() {
      super(EvalType.CONST);
    }

    @Override
    public Datum eval(Schema schema, Tuple tuple) {
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
    public DataType getValueType() {
      return CatalogUtil.newSimpleDataType(BOOLEAN);
    }
  }

  @Test
  public void testAndTest() {
    MockTrueEval trueExpr = new MockTrueEval();
    MockFalseExpr falseExpr = new MockFalseExpr();

    BinaryEval andExpr = new BinaryEval(EvalType.AND, trueExpr, trueExpr);
    assertTrue(andExpr.eval(null, null).asBool());

    andExpr = new BinaryEval(EvalType.AND, falseExpr, trueExpr);
    assertFalse(andExpr.eval(null, null).asBool());

    andExpr = new BinaryEval(EvalType.AND, trueExpr, falseExpr);
    assertFalse(andExpr.eval(null, null).asBool());

    andExpr = new BinaryEval(EvalType.AND, falseExpr, falseExpr);
    assertFalse(andExpr.eval(null, null).asBool());
  }

  @Test
  public void testOrTest() {
    MockTrueEval trueExpr = new MockTrueEval();
    MockFalseExpr falseExpr = new MockFalseExpr();

    BinaryEval orExpr = new BinaryEval(EvalType.OR, trueExpr, trueExpr);
    assertTrue(orExpr.eval(null, null).asBool());

    orExpr = new BinaryEval(EvalType.OR, falseExpr, trueExpr);
    assertTrue(orExpr.eval(null, null).asBool());

    orExpr = new BinaryEval(EvalType.OR, trueExpr, falseExpr);
    assertTrue(orExpr.eval(null, null).asBool());

    orExpr = new BinaryEval(EvalType.OR, falseExpr, falseExpr);
    assertFalse(orExpr.eval(null, null).asBool());
  }

  @Test
  public final void testCompOperator() {
    ConstEval e1;
    ConstEval e2;
    BinaryEval expr;

    // Constant
    e1 = new ConstEval(DatumFactory.createInt4(9));
    e2 = new ConstEval(DatumFactory.createInt4(34));
    expr = new BinaryEval(EvalType.LTH, e1, e2);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.LEQ, e1, e2);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.LTH, e2, e1);
    assertFalse(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.LEQ, e2, e1);
    assertFalse(expr.eval(null, null).asBool());

    expr = new BinaryEval(EvalType.GTH, e2, e1);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.GEQ, e2, e1);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.GTH, e1, e2);
    assertFalse(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.GEQ, e1, e2);
    assertFalse(expr.eval(null, null).asBool());

    BinaryEval plus = new BinaryEval(EvalType.PLUS, e1, e2);
    expr = new BinaryEval(EvalType.LTH, e1, plus);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.LEQ, e1, plus);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.LTH, plus, e1);
    assertFalse(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.LEQ, plus, e1);
    assertFalse(expr.eval(null, null).asBool());

    expr = new BinaryEval(EvalType.GTH, plus, e1);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.GEQ, plus, e1);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.GTH, e1, plus);
    assertFalse(expr.eval(null, null).asBool());
    expr = new BinaryEval(EvalType.GEQ, e1, plus);
    assertFalse(expr.eval(null, null).asBool());
  }

  @Test
  public final void testArithmaticsOperator() 
      throws CloneNotSupportedException {
    ConstEval e1;
    ConstEval e2;

    // PLUS
    e1 = new ConstEval(DatumFactory.createInt4(9));
    e2 = new ConstEval(DatumFactory.createInt4(34));
    BinaryEval expr = new BinaryEval(EvalType.PLUS, e1, e2);
    assertEquals(expr.eval(null, null).asInt4(), 43);
    assertCloneEqual(expr);
    
    // MINUS
    e1 = new ConstEval(DatumFactory.createInt4(5));
    e2 = new ConstEval(DatumFactory.createInt4(2));
    expr = new BinaryEval(EvalType.MINUS, e1, e2);
    assertEquals(expr.eval(null, null).asInt4(), 3);
    assertCloneEqual(expr);
    
    // MULTIPLY
    e1 = new ConstEval(DatumFactory.createInt4(5));
    e2 = new ConstEval(DatumFactory.createInt4(2));
    expr = new BinaryEval(EvalType.MULTIPLY, e1, e2);
    assertEquals(expr.eval(null, null).asInt4(), 10);
    assertCloneEqual(expr);
    
    // DIVIDE
    e1 = new ConstEval(DatumFactory.createInt4(10));
    e2 = new ConstEval(DatumFactory.createInt4(5));
    expr = new BinaryEval(EvalType.DIVIDE, e1, e2);
    assertEquals(expr.eval(null, null).asInt4(), 2);
    assertCloneEqual(expr);
  }

  @Test
  public final void testGetReturnType() {
    ConstEval e1;
    ConstEval e2;

    // PLUS
    e1 = new ConstEval(DatumFactory.createInt4(9));
    e2 = new ConstEval(DatumFactory.createInt4(34));
    BinaryEval expr = new BinaryEval(EvalType.PLUS, e1, e2);
    assertEquals(CatalogUtil.newSimpleDataType(INT4), expr.getValueType());

    expr = new BinaryEval(EvalType.LTH, e1, e2);
    assertTrue(expr.eval(null, null).asBool());
    assertEquals(CatalogUtil.newSimpleDataType(BOOLEAN), expr.getValueType());

    e1 = new ConstEval(DatumFactory.createFloat8(9.3));
    e2 = new ConstEval(DatumFactory.createFloat8(34.2));
    expr = new BinaryEval(EvalType.PLUS, e1, e2);
    assertEquals(CatalogUtil.newSimpleDataType(FLOAT8), expr.getValueType());
  }
  
  @Test
  public final void testEquals() throws CloneNotSupportedException {
    ConstEval e1;
    ConstEval e2;

    // PLUS
    e1 = new ConstEval(DatumFactory.createInt4(34));
    e2 = new ConstEval(DatumFactory.createInt4(34));
    assertEquals(e1, e2);
    
    BinaryEval plus1 = new BinaryEval(EvalType.PLUS, e1, e2);
    BinaryEval plus2 = new BinaryEval(EvalType.PLUS, e2, e1);
    assertEquals(plus1, plus2);
    
    ConstEval e3 = new ConstEval(DatumFactory.createInt4(29));
    BinaryEval plus3 = new BinaryEval(EvalType.PLUS, e1, e3);
    assertFalse(plus1.equals(plus3));
    
    // LTH
    ConstEval e4 = new ConstEval(DatumFactory.createInt4(9));
    ConstEval e5 = new ConstEval(DatumFactory.createInt4(34));
    BinaryEval compExpr1 = new BinaryEval(EvalType.LTH, e4, e5);
    assertCloneEqual(compExpr1);
    
    ConstEval e6 = new ConstEval(DatumFactory.createInt4(9));
    ConstEval e7 = new ConstEval(DatumFactory.createInt4(34));
    BinaryEval compExpr2 = new BinaryEval(EvalType.LTH, e6, e7);
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
    
    BinaryEval plus1 = new BinaryEval(EvalType.PLUS, e1, e2);
    assertCloneEqual(plus1);
    BinaryEval plus2 = new BinaryEval(EvalType.PLUS, e2, e1);
    assertCloneEqual(plus2);
    BinaryEval plus3 = new BinaryEval(EvalType.PLUS, plus2, plus1);
    assertCloneEqual(plus3);
    
    ConstEval e3 = new ConstEval(DatumFactory.createInt4(29));
    BinaryEval gth = new BinaryEval(EvalType.GTH, e3, plus3);
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
}
