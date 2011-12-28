/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;

import nta.catalog.Catalog;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableInfo;
import nta.engine.plan.logical.InsertIntoLO;
import nta.engine.query.TargetEntry;
import nta.storage.StorageManager;
import nta.storage.UpdatableScanner;
import nta.storage.VTuple;

/**
 * @author hyunsik
 *
 */
public class InsertIntoOp extends PhysicalOp {
	final Catalog cat;
	final StorageManager sm;
	final InsertIntoLO lo;
	/**
	 * 
	 */
	public InsertIntoOp(Catalog cat, StorageManager sm, InsertIntoLO lo) {
		this.cat = cat;
		this.sm = sm;
		this.lo = lo;
	}

	@Override
	public Schema getSchema() {
		return null;
	}

	/* (non-Javadoc)
	 * @see nta.engine.exec.PhysicalOp#next()
	 */
	@Override
	public VTuple next() throws IOException {		
		TableInfo info = cat.getTableInfo(lo.getTableId());
		UpdatableScanner sc = sm.getUpdatableScanner(info.getStore()); 
		VTuple tuple = new VTuple(info.getSchema().getColumnNum());
		
		TargetEntry [] targets = lo.getTargets();
		
		Schema schema = info.getSchema();
		for(int i=0; i < targets.length; i++) {
			Column col = schema.getColumn(targets[i].colName);
			switch(col.getDataType()) {
			case BOOLEAN:
				tuple.put(col.getId(), lo.getValues()[i].asBool()); break;
			case SHORT:
				tuple.put(col.getId(), lo.getValues()[i].asShort()); break;
			case INT:
				tuple.put(col.getId(), lo.getValues()[i].asInt()); break;
			case FLOAT:
				tuple.put(col.getId(), lo.getValues()[i].asFloat()); break;
			case DOUBLE:
				tuple.put(col.getId(), lo.getValues()[i].asDouble()); break;			
			case STRING:
				tuple.put(col.getId(), lo.getValues()[i].asChars()); break;				
			}
		}
		
		sc.addTuple(tuple);		
		sc.close();
		
		return null;
	}
}
