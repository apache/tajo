/**
 * 
 */
package nta.engine.executor.eval;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;

/**
 * @author Hyunsik Choi
 *
 */
public class ExprFactory {
	public static ConstExpr newConst(Datum datum) {
		return new ConstExpr(datum);
	}
	
	public static BinaryExpr newBinaryEval(ExprType type, Expr e1, Expr e2) {
		return new BinaryExpr(type, e1, e2);
	}
	
	public static FieldExpr newField(DataType type, int tableId, int fieldId) {
		return new FieldExpr(type, tableId, fieldId);
	}
}
