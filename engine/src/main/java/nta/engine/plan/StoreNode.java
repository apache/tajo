/**
 * 
 */
package nta.engine.plan;

import nta.engine.executor.ResTableExec;

/**
 * @author Hyunsik Choi
 *
 */
public class StoreNode extends PlanNode {
	ResTableExec exec;

	/**
	 * @param type
	 */
	public StoreNode(ResTableExec exec) {
		super(NodeType.Save);		
		this.exec = exec;
	}
	
	public ResTableExec getExecutor() {
		return this.exec;
	}
}
