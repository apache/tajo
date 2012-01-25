/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;
import java.util.Iterator;

import nta.catalog.Catalog;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.datum.DatumFactory;
import nta.engine.function.Function;
import nta.engine.plan.logical.ControlLO;
import nta.storage.VTuple;

/**
 * @author hyunsik
 *
 */
public class ShowFunctionOp extends PhysicalOp {
	ControlLO logicalOp;
	Iterator<FunctionDesc> iterator;
	
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
		
		FunctionDesc desc = this.iterator.next();
		VTuple t = new VTuple(3);
		t.put(0, DatumFactory.createString(desc.getSignature()));
		t.put(1, DatumFactory.createString(desc.getFuncType().toString()));
		t.put(2, DatumFactory.createString(desc.getFuncClass().getName()));
		return t;
	}

}
