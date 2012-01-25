package nta.engine.exec.eval;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.storage.Tuple;

public class AggFuncEval extends EvalNode {
	String funcName;
	Function function;
	EvalNode [] paras = null;

	public AggFuncEval(String funcName, Function instance) {
		super(Type.FUNCTION);
		this.funcName = funcName;
		this.function = instance;
	}
	public AggFuncEval(String funcName, Function instance, EvalNode [] paras) {
		this(funcName, instance);
		this.setParas(paras);
	}
	
	public void setParas(EvalNode [] paras) {
		this.paras = paras;
	}
	
	public DataType getResType() {
		return function.getResType();
	}

	@Override
	public Datum eval(Schema schema, Tuple tuple, Datum...args) {
		Datum [] params = null;
		
		if(paras != null) {
		  params = new Datum[args.length];
			for(int i=0;i < paras.length; i++) {
				params[i] = paras[i].eval(schema, tuple);
			}
		}
		
		return function.invoke(params);
	}

	@Override
	public String getName() {
		return funcName;
	}
	
	@Override
	public DataType getValueType() {
		return DataType.ANY;
	}
  @Override
  public boolean equals(Object obj) {
    return true; // TODO - to be implemented
  }
}
