/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;
import java.util.Iterator;

import nta.catalog.Catalog;
import nta.catalog.FunctionMeta;
import nta.catalog.Schema;
import nta.engine.function.Function;
import nta.engine.plan.logical.ControlLO;
import nta.storage.VTuple;

/**
 * @author hyunsik
 *
 */
public class ShowFunctionOp extends PhysicalOp {
	ControlLO logicalOp;
	Iterator<FunctionMeta> iterator;
	
	/**
	 * 
	 */
	public ShowFunctionOp(ControlLO logicalOp, Catalog cat) {
		this.logicalOp = logicalOp;
		this.iterator = cat.getFunctions().iterator();
	}

	/* (non-Javadoc)
	 * @see nta.SchemaObject#getSchema()
	 */
	@Override
	public Schema getSchema() {
		return logicalOp.getSchema();
	}

	/* (non-Javadoc)
	 * @see nta.query.exec.PhysicalOp#next()
	 */
	@Override
	public VTuple next() throws IOException {
		if(!this.iterator.hasNext())
			return null;
		
		FunctionMeta desc = this.iterator.next();
		VTuple t = new VTuple(3);
		t.put(0, desc.getName());
		t.put(1, desc.getType());
		t.put(2, desc.getFunctionClass().getName());
		return t;
	}

}
