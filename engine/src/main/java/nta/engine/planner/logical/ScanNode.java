/**
 * 
 */
package nta.engine.planner.logical;

import com.google.gson.annotations.Expose;

import nta.engine.exec.eval.EvalNode;
import nta.engine.json.GsonCreator;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.utils.TUtil;

/**
 * @author Hyunsik Choi
 *
 */
public class ScanNode extends LogicalNode {
	@Expose private FromTable table;
	@Expose private EvalNode qual;
	@Expose private QueryBlock.Target[] targets;
	@Expose private boolean local;
	
	public ScanNode() {
		super();
		local = false;
	}
  
	public ScanNode(FromTable table) {
		super(ExprType.SCAN);
		this.table = table;
		this.setInputSchema(table.getSchema());
		this.setOutputSchema(table.getSchema());
		local = false;
	}
	
	public String getTableId() {
	  return table.getTableName();
	}
	
	public boolean hasAlias() {
	  return table.hasAlias();
	}
	
	public boolean hasQual() {
	  return qual != null;
	}
	
	public EvalNode getQual() {
	  return this.qual;
	}
	
	public boolean isLocal() {
	  return this.local;
	}
	
	public void setLocal(boolean local) {
	  this.local = local;
	}
	
	public void setQual(EvalNode evalTree) {
	  this.qual = evalTree;
	}
	
	public boolean hasTargetList() {
	  return this.targets != null;
	}
	
	public void setTargets(Target [] targets) {
	  this.targets = targets;
	}
	
	public Target [] getTargets() {
	  return this.targets;
	}
	
	public String toString() {
	  StringBuilder sb = new StringBuilder();	  
	  sb.append("\"Scan\" : {\"table\":\"")
	  .append(table.getTableName()).append("\"");
	  if (hasAlias()) {
	    sb.append(",\"alias\": \"").append(table.getAlias());
	  }
	  
	  if (hasQual()) {
	    sb.append(", \"qual\": \"").append(this.qual).append("\"");
	  }
	  
	  if (hasTargetList()) {
	    sb.append(", \"target list\": ");
      boolean first = true;
      for (Target target : targets) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(target);
        first = false;
      }
	  }
	  
	  sb.append(",");
	  sb.append("\n  \"out schema\": ").append(getOutputSchema());
	  sb.append("\n  \"in schema\": ").append(getInputSchema());    	  
	  return sb.toString();
	}
	
	public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, LogicalNode.class);
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof ScanNode) {
	    ScanNode other = (ScanNode) obj;
	    
	    boolean eq = super.equals(other); 
	    eq = eq && TUtil.checkEquals(this.table, other.table);
	    eq = eq && TUtil.checkEquals(this.qual, other.qual);
	    eq = eq && TUtil.checkEquals(this.targets, other.targets);
	    
	    return eq;
	  }	  
	  
	  return false;
	}	
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  ScanNode scanNode = (ScanNode) super.clone();
	  
	  scanNode.table = (FromTable) this.table.clone();
	  
	  if (hasQual()) {
	    scanNode.qual = (EvalNode) this.qual.clone();
	  }
	  
	  if (hasTargetList()) {
	    scanNode.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        scanNode.targets[i] = (Target) targets[i].clone();
      }
	  }
	  
	  return scanNode;
	}
	
  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
	
	public void postOrder(LogicalNodeVisitor visitor) {        
    visitor.visit(this);
  }
}
