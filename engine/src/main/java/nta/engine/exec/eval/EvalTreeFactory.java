/**
 * 
 */
package nta.engine.exec.eval;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;

/**
 * @author Hyunsik Choi
 *
 */
public class EvalTreeFactory {
	public static ConstEval newConst(Datum datum) {
		return new ConstEval(datum);
	}
	
	public static BinaryEval newBinaryEval(EvalNode.Type type, EvalNode e1, 
	    EvalNode e2) {
		return new BinaryEval(type, e1, e2);
	}
}
