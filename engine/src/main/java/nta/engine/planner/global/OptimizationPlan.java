/**
 * 
 */
package nta.engine.planner.global;


/**
 * @author jihoon
 *
 */
public class OptimizationPlan {

	// # of steps
	private int stepNum;
	// execution plans for each step
	private String[] execPlan;
	// # of nodes in each step
	private int[] nodeNum;
	// mapping type between each step
	private MappingType[] mappingType;
	
	public OptimizationPlan() {
		
	}
	
	public OptimizationPlan(int stepNum, String[] execPlan, int[] nodeNum, MappingType[] mappingType) {
		this.set(stepNum, execPlan, nodeNum, mappingType);
	}
	
	public void set(int stepNum, String[] execPlan, int[] nodeNum, MappingType[] mappingType) {
		this.stepNum = stepNum;
		this.execPlan = execPlan;
		this.nodeNum = nodeNum;
		this.mappingType = mappingType;
	}
	
	public int getStepNum() {
		return this.stepNum;
	}
	
	public String[] getExecPlan() {
		return this.execPlan;
	}
	
	public int[] getNodeNum() {
		return this.nodeNum;
	}
	
	public MappingType[] getMappingType() {
		return this.mappingType;
	}
}
