package nta.engine.executor.eval;

import nta.datum.Datum;
import nta.storage.Tuple;

public abstract class IncExpr extends Expr {
	
	public IncExpr(ExprType type) {
		super(type);
	}
	
	public abstract Datum eval(Datum cur,Tuple tuple);
}
