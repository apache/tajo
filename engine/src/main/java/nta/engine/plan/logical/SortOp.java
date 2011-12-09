package nta.engine.plan.logical;

public class SortOp extends UnaryOp {
	boolean asc;
	
	public SortOp() {
		super(OpType.SORT);
	}
	
	public boolean ascending() {
		return asc;
	}
}
