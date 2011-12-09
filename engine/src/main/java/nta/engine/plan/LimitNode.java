package nta.engine.plan;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class LimitNode extends PlanNode {
	private long offset;
	private long count;
	
	public LimitNode() {
		super(NodeType.Limit);
	}
	
	public long getOffset() {
		return offset;
	}
	
	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	public long getCount() {
		return this.count;
	}
	
	public void setCount(long count) {
		this.count = count;
	}
}
