/**
 * 
 */
package tajo.engine.exec.eval;

import tajo.datum.Datum;

/**
 * @author Hyunsik Choi
 *
 */
public class EvalTreeFactory {
	public static ConstEval newConst(Datum datum) {
		return new ConstEval(datum);
	}
	
	public static BinaryEval create(EvalNode.Type type, EvalNode e1, 
	    EvalNode e2) {
		return new BinaryEval(type, e1, e2);
	}
}
