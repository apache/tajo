package nta.engine.plan;

import nta.catalog.Schema;
import nta.engine.executor.ScanExec;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class ScanNode extends PlanNode {
	ScanExec executor = null;
	Schema schema = null;
	
	public ScanNode() {
		super(NodeType.Scan);
	}
	
	public ScanNode(ScanExec exec) {
		this();
		this.executor = exec;
	}
	
	public ScanExec getExecutor() {
		return this.executor;
	}
}
