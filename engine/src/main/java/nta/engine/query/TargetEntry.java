package nta.engine.query;

import nta.engine.executor.eval.Expr;

public class TargetEntry {
	public Expr expr;
	public int resId = -1;
	public String qualifier;
	public String colName;
	
	public int relId = -1;
	public int colId = -1;
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
				
		if(qualifier != null) {
			sb.append(qualifier).append(".");
		}
		
		sb.append(colName);
		
		if(resId != -1) {
			sb.append("(> ").append(resId).append(")");
		}
		
		return sb.toString();
	}
}
