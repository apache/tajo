/**
 * 
 */
package nta.engine.plan;

import java.util.ArrayList;
import java.util.List;

import nta.engine.exec.eval.EvalNode;

/**
 * @author Hyunsik Choi
 *
 */
public class PrjNode extends PlanNode {
	boolean asterik = false;
	List<EvalNode> selList = new ArrayList<EvalNode>();

	public PrjNode() {
		super(NodeType.Project);
	}
	
	public void setAsterik() {
		this.asterik = true;
	}
	
	public void addExpr(EvalNode e) {
		selList.add(e);
	}
	
	public List<EvalNode> getSelList() {
		return this.selList;
	}
}
