package nta.engine.executor.expr;

import static org.junit.Assert.assertEquals;
import nta.catalog.proto.TableProtos.DataType;
import nta.datum.DatumFactory;
import nta.engine.executor.eval.BinaryExpr;
import nta.engine.executor.eval.ConstExpr;
import nta.engine.executor.eval.ExprType;
import nta.engine.executor.eval.FieldExpr;
import nta.storage.VTuple;

import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestFieldExpr {

	@Test
	public void testEvalVal() {
		ConstExpr e1 = new ConstExpr(DatumFactory.create(1));	
		FieldExpr e2 = new FieldExpr(DataType.INT, 1, 4); // it indicates 4th field. 
		
		BinaryExpr expr = new BinaryExpr(ExprType.PLUS, e1, e2);
		VTuple tuple = new VTuple(6);
		tuple.put(4, 99); // put 0th field
		
		// the result of evaluation must be 100.
		assertEquals(expr.eval(tuple).asInt(), 100);
	}

}
