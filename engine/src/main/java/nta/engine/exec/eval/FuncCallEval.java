/**
 * 
 */
package nta.engine.exec.eval;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 *
 */
public class FuncCallEval extends EvalNode {
	Function instance;

	/**
	 * @param type
	 */
	public FuncCallEval(Function instance) {
		super(Type.FUNCTION);
		this.instance = instance;
	}
	
	public DataType getValueType() {
		return instance.getResType();
	}

	/* (non-Javadoc)
	 * @see nta.query.executor.eval.Expr#evalVal(nta.storage.Tuple)
	 */
	@Override
	public Datum eval(Tuple tuple, Datum...args) {
		EvalNode [] param = instance.getGivenArgs();
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
		return instance.getSignature();
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		EvalNode e = null;
		for(int i=0; i < instance.getGivenArgs().length; i++) {
			sb.append(instance.getGivenArgs()[i]);
			if(i+1 < instance.getGivenArgs().length)
				sb.append(",");
		}
		return instance.getSignature()+"("+sb+")";
	}
}
