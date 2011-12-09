package nta.engine.executor.eval;

import java.util.List;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.storage.Tuple;

public class AggFuncExpr extends IncExpr {
	String funcName;
	Function function;
	List<Expr> paras = null;

	public AggFuncExpr(String funcName, Function instance) {
		super(ExprType.FUNCTION);
		this.funcName = funcName;
		this.function = instance;
	}
	public AggFuncExpr(String funcName, Function instance, List<Expr> paras) {
		this(funcName, instance);
		this.setParas(paras);
	}
	
	public void setParas(List<Expr> paras) {
		this.paras = paras;
	}
	
	public DataType getResType() {
		return function.getResType();
	}

	@Override
	public Datum eval(Datum cur, Tuple tuple) {
		Datum data = null;
		
		if(paras != null) {

			for(int i=0;i < paras.size(); i++) {
				data = function.invoke(paras.get(i).eval(tuple));
			}
		}

		return data;
	}

	@Override
	public String getName() {
		return funcName;
	}

	@Override
	public Datum eval(Tuple tuple) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public DataType getValueType() {
		return DataType.ANY;
	}

}
