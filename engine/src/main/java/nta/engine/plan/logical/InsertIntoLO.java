/**
 * 
 */
package nta.engine.plan.logical;

import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.query.TargetEntry;

/**
 * @author hyunsik
 *
 */
public class InsertIntoLO extends LogicalOp {
	final int tableId;
	final TargetEntry [] targets;
	final Datum [] values; 
	
	/**
	 * @param opType
	 */
	public InsertIntoLO(int tableId, TargetEntry [] targets, Datum [] values) {
		super(OpType.INSERT_INTO);
		this.tableId = tableId;
		this.targets = targets;
		this.values = values;
	}
	
	public int getTableId() {
		return this.tableId;
	}
	
	public Datum [] getValues() {
		return this.values;
	}
	
	public TargetEntry [] getTargets() {
		return this.targets;
	}

	/* (non-Javadoc)
	 * @see nta.engine.plan.logical.LogicalOp#getSchema()
	 */
	@Override
	public Schema getSchema() {		
		return null;
	}
}
