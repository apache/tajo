/**
 * 
 */
package nta.distexec;

import nta.common.ProtoObject;
import nta.engine.ExecutorRunnerProtos.DistPlanContainerProto;
import nta.engine.ExecutorRunnerProtos.ExecType;

/**
 * @author jihoon
 *
 */
public class DistPlanContainer implements ProtoObject<DistPlanContainerProto> {
	
	private ExecType type;
	private DistPlan plan;
	
	public DistPlanContainer() {
		
	}
	
	public DistPlanContainer(ExecType type) {
		this.setType(type);
	}
	
	public DistPlanContainer(ExecType type, DistPlan plan) {
		set(type, plan);
	}
	
	public void setType(ExecType type) {
		this.type = type;
		switch (this.type) {
		case CUBEBY:
			this.plan = new DistCubeByPlan();
			break;
		case GROUPBY:
			this.plan = new DistGroupByPlan();
			break;
		case JOIN:
			this.plan = new DistJoinPlan();
			break;
		case SORT:
			this.plan = new DistSortPlan();
			break;
		}
	}
	
	public void set(ExecType type, DistPlan plan) {
		this.type = type;
		this.plan = plan;
	}
	
	public ExecType getType() {
		return this.type;
	}
	
	public DistPlan getPlan() {
		return this.plan;
	}

	@Override
	public DistPlanContainerProto getProto() {
		// TODO Auto-generated method stub
		return null;
	}
}
