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
	private Function instance;
	private EvalNode [] givenArgs;

	/**
	 * @param type
	 */
	public FuncCallEval(Function instance, EvalNode [] givenArgs) {
		super(Type.FUNCTION);
		this.instance = instance;
		this.givenArgs = givenArgs;
	}
	
	public DataType getValueType() {
		return instance.getResType();
	}

	/* (non-Javadoc)
	 * @see nta.query.executor.eval.Expr#evalVal(nta.storage.Tuple)
	 */
	@Override
	public Datum eval(Tuple tuple, Datum...args) {		
		Datum [] data = null;
		
		if(givenArgs != null) {
			data = new Datum[givenArgs.length];

			for(int i=0;i < givenArgs.length; i++) {
				data[i] = givenArgs[i].eval(tuple);
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
		for(int i=0; i < givenArgs.length; i++) {
			sb.append(givenArgs[i]);
			if(i+1 < givenArgs.length)
				sb.append(",");
		}
		return instance.getSignature()+"("+sb+")";
	}
}
