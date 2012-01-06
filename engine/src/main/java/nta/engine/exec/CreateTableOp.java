/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;

import nta.catalog.Catalog;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.exception.AlreadyExistsTableException;
import nta.engine.plan.logical.CreateTableLO;
import nta.storage.StorageManager;
import nta.storage.VTuple;

/**
 * @author hyunsik
 *
 */
public class CreateTableOp extends PhysicalOp {
	private final CreateTableLO logicalOp;
	private final Catalog cat;
	private final StorageManager sm;
	
	public CreateTableOp(CreateTableLO op, Catalog cat, StorageManager sm) {
		this.logicalOp = op;
		this.cat = cat;
		this.sm = sm;
	}

	/* (non-Javadoc)
	 * @see nta.SchemaObject#getSchema()
	 */
	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see nta.query.exec.PhysicalOp#next()
	 */
	@Override
	public VTuple next() throws IOException {
		String tableName = this.logicalOp.getTableName();
		if(cat.existsTable(tableName)) {
			throw new AlreadyExistsTableException(tableName);
		} else {
			
			TableMeta meta = new TableMetaImpl();			
			meta.setSchema(logicalOp.getTableMeta());
			meta.setStorageType(logicalOp.getStoreType());
			
			TableDesc desc = new TableDescImpl(tableName, meta);
			
//			Store store = sm.create(meta);
//			meta.setStore(store);
			
			cat.addTable(desc);
		}
		
		return null;
	}

}
