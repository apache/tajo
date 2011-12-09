package nta.engine.executor.expr;

import static org.junit.Assert.*;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.executor.eval.BinaryExpr;
import nta.engine.executor.eval.ConstExpr;
import nta.engine.executor.eval.Expr;
import nta.engine.executor.eval.ExprType;
import nta.storage.Tuple;

import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestBinaryExpr {
public static class MockTrueExpr extends Expr {
		
		public MockTrueExpr() {
			super(ExprType.CONST);
		}

		@Override
		public Datum eval(Tuple tuple) {
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
	
	public static class MockFalseExpr extends Expr {
		
		public MockFalseExpr() {
			super(ExprType.CONST);
		}

		@Override
		public Datum eval(Tuple tuple) {
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
		MockTrueExpr trueExpr = new MockTrueExpr();
		MockFalseExpr falseExpr = new MockFalseExpr();
		
		BinaryExpr andExpr = new BinaryExpr(ExprType.AND, trueExpr, trueExpr);
		assertTrue(andExpr.eval(null).asBool());
		
		andExpr = new BinaryExpr(ExprType.AND, falseExpr, trueExpr);
		assertFalse(andExpr.eval(null).asBool());
		
		andExpr = new BinaryExpr(ExprType.AND,trueExpr, falseExpr);
		assertFalse(andExpr.eval(null).asBool());
		
		andExpr = new BinaryExpr(ExprType.AND,falseExpr, falseExpr);
		assertFalse(andExpr.eval(null).asBool());
	}
	
	@Test
	public void testOrTest() {
		MockTrueExpr trueExpr = new MockTrueExpr();
		MockFalseExpr falseExpr = new MockFalseExpr();
		
		BinaryExpr orExpr = new BinaryExpr(ExprType.OR,trueExpr, trueExpr);
		assertTrue(orExpr.eval(null).asBool());
		
		orExpr = new BinaryExpr(ExprType.OR,falseExpr, trueExpr);
		assertTrue(orExpr.eval(null).asBool());
		
		orExpr = new BinaryExpr(ExprType.OR,trueExpr, falseExpr);
		assertTrue(orExpr.eval(null).asBool());
		
		orExpr = new BinaryExpr(ExprType.OR,falseExpr, falseExpr);
		assertFalse(orExpr.eval(null).asBool());
	}
	
	@Test
	public final void testCompOperator() {
		ConstExpr e1 = null;
		ConstExpr e2 = null;	
		BinaryExpr expr = null;
		
		// Constant
		e1 = new ConstExpr(DatumFactory.create(9));
		e2 = new ConstExpr(DatumFactory.create(34));
		expr = new BinaryExpr(ExprType.LTH, e1, e2);		
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.LEQ, e1, e2);		
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.LTH, e2, e1);
		assertFalse(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.LEQ, e2, e1);
		assertFalse(expr.eval(null).asBool());
		
		expr = new BinaryExpr(ExprType.GTH, e2, e1);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.GEQ, e2, e1);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.GTH, e1, e2);
		assertFalse(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.GEQ, e1, e2);
		assertFalse(expr.eval(null).asBool());
		
		BinaryExpr plus = new BinaryExpr(ExprType.PLUS, e1, e2);
		expr = new BinaryExpr(ExprType.LTH, e1, plus);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.LEQ, e1, plus);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.LTH, plus, e1);
		assertFalse(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.LEQ, plus, e1);
		assertFalse(expr.eval(null).asBool());
		
		expr = new BinaryExpr(ExprType.GTH, plus, e1);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.GEQ, plus, e1);
		assertTrue(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.GTH, e1, plus);
		assertFalse(expr.eval(null).asBool());
		expr = new BinaryExpr(ExprType.GEQ, e1, plus);
		assertFalse(expr.eval(null).asBool());
	}

	@Test
	public final void testArithmaticsOperator() {
		ConstExpr e1 = null;
		ConstExpr e2 = null;	
		
		// PLUS
		e1 = new ConstExpr(DatumFactory.create(9));
		e2 = new ConstExpr(DatumFactory.create(34));
		BinaryExpr expr = new BinaryExpr(ExprType.PLUS, e1, e2);		
		assertEquals(expr.eval(null).asInt(), 43);
		
		// MINUS
		e1 = new ConstExpr(DatumFactory.create(5));
		e2 = new ConstExpr(DatumFactory.create(2));
		expr = new BinaryExpr(ExprType.MINUS, e1, e2);
		assertEquals(expr.eval(null).asInt(), 3);
		
		// MULTIPLY
		e1 = new ConstExpr(DatumFactory.create(5));
		e2 = new ConstExpr(DatumFactory.create(2));
		expr = new BinaryExpr(ExprType.MULTIPLY, e1, e2);
		assertEquals(expr.eval(null).asInt(), 10);
		
		// DIVIDE
		e1 = new ConstExpr(DatumFactory.create(10));
		e2 = new ConstExpr(DatumFactory.create(5));
		expr = new BinaryExpr(ExprType.DIVIDE, e1, e2);
		assertEquals(expr.eval(null).asInt(), 2);
	}
	
	@Test
	public final void testGetReturnType() {
		ConstExpr e1 = null;
		ConstExpr e2 = null;	
		
		// PLUS
		e1 = new ConstExpr(DatumFactory.create(9));
		e2 = new ConstExpr(DatumFactory.create(34));
		BinaryExpr expr = new BinaryExpr(ExprType.PLUS, e1, e2);	
		assertEquals(DataType.INT, expr.getValueType());
		
		expr = new BinaryExpr(ExprType.LTH, e1, e2);		
		assertTrue(expr.eval(null).asBool());
		assertEquals(DataType.BOOLEAN, expr.getValueType());
		
		e1 = new ConstExpr(DatumFactory.create(9.3));
		e2 = new ConstExpr(DatumFactory.create(34.2));
		expr = new BinaryExpr(ExprType.PLUS, e1, e2);
		assertEquals(DataType.DOUBLE, expr.getValueType());
	}
}
