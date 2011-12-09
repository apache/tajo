/**
 * 
 */
package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.executor.eval.Expr;

/**
 * @author Hyunsik Choi
 *
 */
public class UnixTimeFunc extends Function {
	
	public UnixTimeFunc(Column[] paraInfo, Expr[] params) {
		super(paraInfo, params);		
	}

	/* (non-Javadoc)
	 * @see nta.query.function.Function#invoke(nta.common.datum.Datum[])
	 */
	@Override
	public Datum invoke(Datum... datums) {
		return DatumFactory.create(System.currentTimeMillis());
	}

	@Override
	public DataType getResType() {
		return DataType.LONG;
	}
}
