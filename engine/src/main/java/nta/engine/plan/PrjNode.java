/**
 * 
 */
package nta.engine.plan;

import java.util.ArrayList;
import java.util.List;

import nta.engine.executor.eval.Expr;

/**
 * @author Hyunsik Choi
 *
 */
public class PrjNode extends PlanNode {
	boolean asterik = false;
	List<Expr> selList = new ArrayList<Expr>();

	public PrjNode() {
		super(NodeType.Project);
	}
	
	public void setAsterik() {
		this.asterik = true;
	}
	
	public void addExpr(Expr e) {
		selList.add(e);
	}
	
	public List<Expr> getSelList() {
		return this.selList;
	}
}
