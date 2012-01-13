package nta.engine.plan.logical;

import nta.catalog.Schema;
import nta.engine.exec.eval.EvalNode;
import nta.engine.plan.JoinType;

public class JoinOp extends BinaryOp {

	private JoinType joinType;
	private EvalNode condition;

	public JoinOp(JoinType joinType) {
		super(OpType.JOIN);
	}
	
	public EvalNode getCondition() {
		return this.condition;
	}

	public void setCondition(EvalNode value) {
		this.condition = value;
	}
	
	public JoinType getJoinType() {
		return this.joinType;
	}

	@Override
	public Schema getSchema() {
		return inner.getSchema();
	}
}
