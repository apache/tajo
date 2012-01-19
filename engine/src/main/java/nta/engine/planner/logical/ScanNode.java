/**
 * 
 */
package nta.engine.planner.logical;

import nta.catalog.Schema;
import nta.engine.parser.QueryBlock.FromTable;

/**
 * @author Hyunsik Choi
 *
 */
public class ScanNode extends LogicalNode {
  FromTable table;
  
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
	  sb.append("Table scan (id=")
	  .append(table.getTableId()).append(", name=").append(table.getTableId());
	  if(hasAlias()) {
	    sb.append(", alias=").append(table.getAlias());
	  }
	  sb.append(")");
	  
	  return sb.toString();
	}

  @Override
  public Schema getOutputSchema() {    
    return table.getSchema();
  }
}
