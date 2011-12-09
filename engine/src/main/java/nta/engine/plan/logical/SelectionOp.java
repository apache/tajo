package nta.engine.plan.logical;

import nta.engine.executor.eval.Expr;

public class SelectionOp extends UnaryOp {

	private Expr qual;
	
	public SelectionOp() {
		super(OpType.SELECTION);
	}

	public Expr getQual() {
		return this.qual;
	}

	public void setQual(Expr value) {
		this.qual = value;
	}
}
