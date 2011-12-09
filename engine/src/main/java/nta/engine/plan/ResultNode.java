package nta.engine.plan;

public class ResultNode extends PlanNode {
	
	String tableName = null;

	public ResultNode() {
		super(NodeType.Result);
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return this.tableName;
	}
}
