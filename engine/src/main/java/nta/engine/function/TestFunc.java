/**
 * 
 */
package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.engine.executor.eval.Expr;

/**
 * @author Hyunsik Choi
 *
 */
public class TestFunc extends Function {

	public TestFunc(Column[] paraInfo, Expr[] params) {
		super(paraInfo, params);		
	}

	@Override
	public Datum invoke(Datum... datums) {
		return datums[0];
	}

	@Override
	public DataType getResType() {
		return paramInfo[0].getDataType();
	}
}
