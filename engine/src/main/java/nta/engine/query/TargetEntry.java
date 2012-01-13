package nta.engine.query;

import nta.engine.exec.eval.EvalNode;

public class TargetEntry {
	public EvalNode expr;
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
