package tajo.engine.query;

import tajo.engine.exec.eval.EvalNode;

public class TargetEntry {
	public EvalNode expr;
	public int resId = -1;
	public String qualifier;
	public String colName;
	
	public String relId = null;
	public String colId = null;
	
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
