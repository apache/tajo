/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;
import java.util.Iterator;

import nta.catalog.Catalog;
import nta.catalog.Schema;
import nta.catalog.TableInfo;
import nta.engine.plan.logical.ControlLO;
import nta.storage.VTuple;

/**
 * @author hyunsik
 *
 */
public class ShowTableOp extends PhysicalOp {
	ControlLO logicalOp;
	Iterator<TableInfo> iterator;
	
	/**
	 * 
	 */
	public ShowTableOp(ControlLO logicalOp, Catalog cat) {
		this.logicalOp = logicalOp;
		this.iterator = cat.getTableInfos().iterator();
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
		
		TableInfo desc = this.iterator.next();
		VTuple t = new VTuple(1);
		t.put(0, desc.getName());
		return t;
	}

}
