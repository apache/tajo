/**
 * 
 */
package nta.engine.executor.eval;

import java.util.List;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 *
 */
public class FuncExpr extends Expr {
	String funcName;
	Function instance;

	/**
	 * @param type
	 */
	public FuncExpr(String funcName, Function instance) {
		super(ExprType.FUNCTION);
		this.funcName = funcName;
		this.instance = instance;
	}
	
	public DataType getValueType() {
		return instance.getResType();
	}

	/* (non-Javadoc)
	 * @see nta.query.executor.eval.Expr#evalVal(nta.storage.Tuple)
	 */
	@Override
	public Datum eval(Tuple tuple) {
		Expr [] param = instance.getParams();
		Datum [] data = null;
		
		if(param != null) {
			data = new Datum[param.length];

			for(int i=0;i < param.length; i++) {
				data[i] = param[i].eval(tuple);
			}
		}

		return instance.invoke(data);
	}

	@Override
	public String getName() {		
		return funcName;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		Expr e = null;
		for(int i=0; i < instance.getParams().length; i++) {
			sb.append(instance.getParams()[i]);
			if(i+1 < instance.getParams().length)
				sb.append(",");
		}
		return funcName+"("+sb+")";
	}
}
