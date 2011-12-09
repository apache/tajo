package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.engine.executor.eval.Expr;


public abstract class Function {
	protected Expr [] params;
	protected Column [] paramInfo;
	
	public Function(Column [] paraInfo, Expr [] params) {		
		this.paramInfo = paraInfo;
		this.params = params;
	}
	
	public Column [] getParamInfo() {
		return this.paramInfo;
	}
	
	public Expr [] getParams() {
		return this.params;
	}
	
	public abstract Datum invoke(Datum ... datums);
	public abstract DataType getResType();
}
