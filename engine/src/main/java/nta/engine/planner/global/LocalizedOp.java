/**
 * 
 */
package nta.engine.planner.global;

import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;

/**
 * @author jihoon
 *
 */
public class LocalizedOp {

	private OpType type;
	private LogicalNode logicalNode;
	
	public LocalizedOp() {
		
	}
	
	public LocalizedOp(OpType type, LogicalNode logicalNode) {
		set(type, logicalNode);
	}
	
	public void set(OpType type, LogicalNode logicalNode) {
		this.type = type;
		this.logicalNode = logicalNode;
	}
	
	public OpType getType() {
		return this.type;
	}
	
	public LogicalNode getLogicalNode() {
		return this.logicalNode;
	}
	
}
