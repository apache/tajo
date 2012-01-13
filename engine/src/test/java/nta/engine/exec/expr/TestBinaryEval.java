package nta.engine.exec.expr;

import static org.junit.Assert.*;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.exec.eval.BinaryEval;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.EvalNode.Type;
import nta.storage.Tuple;

import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestBinaryEval {
public static class MockTrueEval extends EvalNode {
		
		public MockTrueEval() {
			super(Type.CONST);
		}

		@Override
		public Datum eval(Tuple tuple, Datum...args) {
			return DatumFactory.create(true);
		}

		@Override
		public String getName() {
			return this.getClass().getName();
		}

		@Override
		public DataType getValueType() {
			return DataType.BOOLEAN;
		}
		
	}
	
	public static class MockFalseExpr extends EvalNode {
		
		public MockFalseExpr() {
			super(Type.CONST);
		}

		@Override
		public Datum eval(Tuple tuple, Datum...args) {
			return DatumFactory.create(false);
		}
		
		@Override
		public String getName() {
			return this.getClass().getName();
		}

		@Override
		public DataType getValueType() {
			return DataType.BOOLEAN;
		}		
	}
	
	@Test
	public void testAndTest() {
		MockTrueEval trueExpr = new MockTrueEval();
		MockFalseExpr falseExpr = new MockFalseExpr();
		
		BinaryEval andExpr = new BinaryEval(Type.AND, trueExpr, trueExpr);
		assertTrue(andExpr.eval(null).asBool());
		
		andExpr = new BinaryEval(Type.AND, falseExpr, trueExpr);
		assertFalse(andExpr.eval(null).asBool());
		
		andExpr = new BinaryEval(Type.AND,trueExpr, falseExpr);
		assertFalse(andExpr.eval(null).asBool());
		
		andExpr = new BinaryEval(Type.AND,falseExpr, falseExpr);
		assertFalse(andExpr.eval(null).asBool());
	}
	
	@Test
	public void testOrTest() {
		MockTrueEval trueExpr = new MockTrueEval();
		MockFalseExpr falseExpr = new MockFalseExpr();
		
		BinaryEval orExpr = new BinaryEval(Type.OR,trueExpr, trueExpr);
		assertTrue(orExpr.eval(null).asBool());
		
		orExpr = new BinaryEval(Type.OR,falseExpr, trueExpr);
		assertTrue(orExpr.eval(null).asBool());
		
		orExpr = new BinaryEval(Type.OR,trueExpr, falseExpr);
		assertTrue(orExpr.eval(null).asBool());
		
		orExpr = new BinaryEval(Type.OR,falseExpr, falseExpr);
		assertFalse(orExpr.eval(null).asBool());
	}
	
	@Test
	public final void testCompOperator() {
		ConstEval e1 = null;
		ConstEval e2 = null;	
		BinaryEval expr = null;
		
		// Constant
		e1 = new ConstEval(DatumFactory.create(9));
		e2 = new ConstEval(DatumFactory.create(34));
		expr = new BinaryEval(Type.LTH, e1, e2);		
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryEval(Type.LEQ, e1, e2);		
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryEval(Type.LTH, e2, e1);
		assertFalse(expr.eval(null).asBool());
		expr = new BinaryEval(Type.LEQ, e2, e1);
		assertFalse(expr.eval(null).asBool());
		
		expr = new BinaryEval(Type.GTH, e2, e1);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryEval(Type.GEQ, e2, e1);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryEval(Type.GTH, e1, e2);
		assertFalse(expr.eval(null).asBool());
		expr = new BinaryEval(Type.GEQ, e1, e2);
		assertFalse(expr.eval(null).asBool());
		
		BinaryEval plus = new BinaryEval(Type.PLUS, e1, e2);
		expr = new BinaryEval(Type.LTH, e1, plus);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryEval(Type.LEQ, e1, plus);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryEval(Type.LTH, plus, e1);
		assertFalse(expr.eval(null).asBool());
		expr = new BinaryEval(Type.LEQ, plus, e1);
		assertFalse(expr.eval(null).asBool());
		
		expr = new BinaryEval(Type.GTH, plus, e1);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryEval(Type.GEQ, plus, e1);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryEval(Type.GTH, e1, plus);
		assertFalse(expr.eval(null).asBool());
		expr = new BinaryEval(Type.GEQ, e1, plus);
		assertFalse(expr.eval(null).asBool());
	}

	@Test
	public final void testArithmaticsOperator() {
		ConstEval e1 = null;
		ConstEval e2 = null;	
		
		// PLUS
		e1 = new ConstEval(DatumFactory.create(9));
		e2 = new ConstEval(DatumFactory.create(34));
		BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);		
		assertEquals(expr.eval(null).asInt(), 43);
		
		// MINUS
		e1 = new ConstEval(DatumFactory.create(5));
		e2 = new ConstEval(DatumFactory.create(2));
		expr = new BinaryEval(Type.MINUS, e1, e2);
		assertEquals(expr.eval(null).asInt(), 3);
		
		// MULTIPLY
		e1 = new ConstEval(DatumFactory.create(5));
		e2 = new ConstEval(DatumFactory.create(2));
		expr = new BinaryEval(Type.MULTIPLY, e1, e2);
		assertEquals(expr.eval(null).asInt(), 10);
		
		// DIVIDE
		e1 = new ConstEval(DatumFactory.create(10));
		e2 = new ConstEval(DatumFactory.create(5));
		expr = new BinaryEval(Type.DIVIDE, e1, e2);
		assertEquals(expr.eval(null).asInt(), 2);
	}
	
	@Test
	public final void testGetReturnType() {
		ConstEval e1 = null;
		ConstEval e2 = null;	
		
		// PLUS
		e1 = new ConstEval(DatumFactory.create(9));
		e2 = new ConstEval(DatumFactory.create(34));
		BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);	
		assertEquals(DataType.INT, expr.getValueType());
		
		expr = new BinaryEval(Type.LTH, e1, e2);		
		assertTrue(expr.eval(null).asBool());
		assertEquals(DataType.BOOLEAN, expr.getValueType());
		
		e1 = new ConstEval(DatumFactory.create(9.3));
		e2 = new ConstEval(DatumFactory.create(34.2));
		expr = new BinaryEval(Type.PLUS, e1, e2);
		assertEquals(DataType.DOUBLE, expr.getValueType());
	}
}
