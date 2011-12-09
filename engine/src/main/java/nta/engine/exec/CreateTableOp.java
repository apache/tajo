/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;

import nta.catalog.Catalog;
import nta.catalog.RelationType;
import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.exception.AlreadyExistsTableException;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.engine.plan.logical.CreateTableLO;
import nta.engine.query.DBContext;
import nta.storage.Store;
import nta.storage.StoreManager;
import nta.storage.VTuple;

/**
 * @author hyunsik
 *
 */
public class CreateTableOp extends PhysicalOp {
	private final CreateTableLO logicalOp;
	private final Catalog cat;
	private final StoreManager sm;
	
	public CreateTableOp(CreateTableLO op, Catalog cat, StoreManager sm) {
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
			
			TableMeta meta = new TableMeta();
			meta.setName(tableName);
			meta.setSchema(logicalOp.getTableMeta());
			meta.setStorageType(logicalOp.getStoreType());
			meta.setTableType(TableType.BASETABLE);
			
			Store store = sm.create(meta);
			meta.setStore(store);
			
			cat.addTable(meta);
		}
		
		return null;
	}

}
