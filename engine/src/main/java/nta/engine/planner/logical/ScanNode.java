/**
 * 
 */
package nta.engine.planner.logical;

import nta.engine.parser.QueryBlock.FromTable;

/**
 * @author Hyunsik Choi
 *
 */
public class ScanNode extends LogicalNode {
  private FromTable table;
  
	public ScanNode(FromTable table) {
		super(ExprType.SCAN);
		this.table = table;
	}
	
	public String getTableId() {
	  return table.getTableId();
	}
	
	public boolean hasAlias() {
	  return table.hasAlias();
	}
	
	public String getAlias() {
	  return table.getAlias();
	}
	
	public String toString() {
	  StringBuilder sb = new StringBuilder();	  
	  sb.append("\"Scan\" : {\"table\":\"")
	  .append(table.getTableId()).append("\"");
	  if(hasAlias()) {
	    sb.append(",\"alias\": \"").append(table.getAlias()).append("\",");
	  }
	  
	  sb.append("\n  \"out schema\": ").append(getOutputSchema());
	  sb.append("\n  \"in schema\": ").append(getInputSchema());    	  
	  return sb.toString();
	}
}
