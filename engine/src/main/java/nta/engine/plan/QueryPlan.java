package nta.engine.plan;

public class QueryPlan {
	PlanNode rootPlan;
	
	public PlanNode getPlanNode() {
		return this.rootPlan;
	}
	
	public void setPlanNode(PlanNode node) {
		this.rootPlan = node;
	}
}
