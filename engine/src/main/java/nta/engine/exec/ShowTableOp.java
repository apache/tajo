/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;
import java.util.Iterator;

import nta.catalog.CatalogServer;
import nta.catalog.CatalogService;
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
	Iterator<String> iterator;
	
	/**
	 * 
	 */
	public ShowTableOp(ControlLO logicalOp, CatalogService cat) {
		this.logicalOp = logicalOp;
		this.iterator = cat.getAllTableNames().iterator();
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
		
		String tableName = this.iterator.next();
		VTuple t = new VTuple(1);
		t.put(0, DatumFactory.createString(tableName));
		return t;
	}

}
