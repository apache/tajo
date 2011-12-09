package nta.engine.plan;

import java.util.ArrayList;
import java.util.List;

import nta.engine.executor.SortExec;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class SortNode extends PlanNode {
	private boolean ascendant = true;
	private List<Integer> sortFields = new ArrayList<Integer>();
	private SortExec executor;
	
	public SortNode() {
		super(NodeType.Sort);
	}
	
	public boolean getAscendant() {
		return this.ascendant;
	}
	
	public void setAscendant() {
		this.ascendant = true;
	}
	
	public void setDecendant() {
		this.ascendant = false;
	}	
	
	public void setSortFields(List<Integer> sortFields) {
		this.sortFields = sortFields;
	}
	
	public List<Integer> getSortFields() {
		return this.sortFields;
	}
	
	public void setExecutor(SortExec exec) {
		this.executor = exec;
	}
	
	public SortExec getExecutor() {
		return this.executor;
	}
}
