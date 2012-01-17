/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.plan.logical.ProjectLO;
import nta.engine.query.TargetEntry;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author hyunsik
 *
 */
public class ProjectOp extends PhysicalOp {
	private final PhysicalOp inner;
	private final ProjectLO logicalOp;
	
	public ProjectOp(PhysicalOp inner, ProjectLO logicalOp) {
		this.inner = inner;
		this.logicalOp = logicalOp;
	}
	
	@Override
	public Schema getSchema() {
		return this.logicalOp.getSchema();
	}

	/* (non-Javadoc)
	 * @see nta.query.exec.PhysicalOp#next()
	 */
	@Override
	public Tuple next() throws IOException {		
		Tuple tuple = inner.next();
		if(tuple == null) {
			return null;
		}
		
		VTuple t = new VTuple(logicalOp.getSchema().getColumns().size());
		TargetEntry [] entries = logicalOp.getTargetList();
		for(TargetEntry entry : entries) {
		  int id = this.logicalOp.getSchema().getColumn(entry.colId).getId();
			t.put(entry.resId, tuple.get(id));			
		}
		
		return t;
	}

}
