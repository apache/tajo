package nta.engine.plan.logical;

import nta.engine.exec.eval.EvalNode;

public class SelectionOp extends UnaryOp {

	private EvalNode qual;
	
	public SelectionOp() {
		super(OpType.SELECTION);
	}

	public EvalNode getQual() {
		return this.qual;
	}

	public void setQual(EvalNode value) {
		this.qual = value;
	}
}
