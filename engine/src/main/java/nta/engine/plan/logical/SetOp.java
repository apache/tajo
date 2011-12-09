package nta.engine.plan.logical;

import nta.catalog.Schema;

public class SetOp extends UnaryOp {
	
	public SetOp() {
		super(OpType.SET_UNION);
	}
}
