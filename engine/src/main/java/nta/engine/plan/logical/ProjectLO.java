package nta.engine.plan.logical;

import nta.catalog.Schema;
import nta.engine.query.TargetEntry;

public class ProjectLO extends UnaryOp {
	private TargetEntry [] targetList;
	private Schema schema;

	public ProjectLO(TargetEntry [] targetList) {		
		super(OpType.PROJECTION);
		this.schema = new Schema();
		this.targetList = targetList;

		for(TargetEntry entry: targetList) {
			schema.addColumn(entry.colName, entry.expr.getValueType());			
		}
	}

	public TargetEntry [] getTargetList() {
		return this.targetList;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}
}
