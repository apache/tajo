package nta.engine.exec.expr;

import static org.junit.Assert.assertEquals;
import nta.catalog.proto.TableProtos.DataType;
import nta.datum.DatumFactory;
import nta.engine.exec.eval.BinaryEval;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.FieldEval;
import nta.engine.exec.eval.EvalNode.Type;
import nta.storage.VTuple;

import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestFieldEval {

	@Test
	public void testEvalVal() {
		ConstEval e1 = new ConstEval(DatumFactory.create(1));	
		FieldEval e2 = new FieldEval(DataType.INT, 1, 4, "field1"); // it indicates 4th field. 
		
		BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
		VTuple tuple = new VTuple(6);
		tuple.put(4, 99); // put 0th field
		
		// the result of evaluation must be 100.
		assertEquals(expr.eval(tuple).asInt(), 100);
	}

}
