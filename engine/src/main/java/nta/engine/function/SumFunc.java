package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.engine.executor.eval.Expr;

public class SumFunc extends Function {
	
	public SumFunc(Column[] paramInfo, Expr[] params) {
		super(paramInfo, params);
	}


	Datum sum;
	
	@Override
	public Datum invoke(Datum... datums) {
		if(sum == null) {
			sum = datums[0];
		} else {
			sum = sum.plus(datums[0]);
		}
		return sum;
	}


	@Override
	public DataType getResType() {
		return paramInfo[0].getDataType();
	}

}
