/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;
import java.util.Iterator;

import nta.catalog.Catalog;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.plan.logical.DescTableLO;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author hyunsik
 *
 */
public class DescTableOp extends PhysicalOp {
	final DescTableLO logicalOp;
	final Iterator<Column> it;
	/**
	 * 
	 */
	public DescTableOp(DescTableLO logicalOp) {
		this.logicalOp = logicalOp;
		this.it = logicalOp.getTargetTableMeta().getColumns().iterator();
	}

	/* (non-Javadoc)
	 * @see nta.SchemaObject#getSchema()
	 */
	@Override
	public Schema getSchema() {
		return this.logicalOp.getSchema();
	}

	/* (non-Javadoc)
	 * @see nta.query.exec.PhysicalOp#next()
	 */
	@Override
	public VTuple next() throws IOException {
		if(!it.hasNext())
			return null;
		
		Column desc = it.next();
		VTuple tuple = new VTuple(3);
		tuple.put(0, desc.getId());
		tuple.put(1, desc.getName());
		tuple.put(2, desc.getDataType().toString());
		return tuple;
	}

}
