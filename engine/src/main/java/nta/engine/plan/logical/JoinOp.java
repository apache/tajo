package nta.engine.plan.logical;

import nta.catalog.Schema;
import nta.engine.executor.eval.Expr;
import nta.engine.plan.JoinType;

public class JoinOp extends BinaryOp {

	private JoinType joinType;
	private Expr condition;

	public JoinOp(JoinType joinType) {
		super(OpType.JOIN);
	}
	
	public Expr getCondition() {
		return this.condition;
	}

	public void setCondition(Expr value) {
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
