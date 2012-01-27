/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;
import java.util.Iterator;

import nta.catalog.CatalogServer;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.datum.DatumFactory;
import nta.engine.plan.logical.ControlLO;
import nta.storage.VTuple;

/**
 * @author hyunsik
 *
 */
public class ShowTableOp extends PhysicalOp {
	ControlLO logicalOp;
	Iterator<TableDesc> iterator;
	
	/**
	 * 
	 */
	public ShowTableOp(ControlLO logicalOp, CatalogServer cat) {
		this.logicalOp = logicalOp;
		this.iterator = cat.getAllTableDescs().iterator();
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
		
		TableDesc desc = this.iterator.next();
		VTuple t = new VTuple(1);
		t.put(0, DatumFactory.createString(desc.getId()));
		return t;
	}

}
