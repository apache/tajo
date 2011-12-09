/**
 * 
 */
package nta.engine.plan.logical;

import nta.catalog.Schema;
import nta.catalog.proto.TableProtos.StoreType;

/**
 * @author hyunsik
 *
 */
public class CreateTableLO extends LogicalOp {
	String tableName;
	Schema meta;
	StoreType storeType = StoreType.MEM;
	LogicalOp subQuery = null;
	
	/**
	 * @param opType
	 */
	public CreateTableLO(String tableName, Schema meta) {
		super(OpType.CREATE_TABLE);
		this.tableName = tableName;
		this.meta = meta;
	}
	
	public CreateTableLO(String tableName, Schema meta, LogicalOp subQuery) {
		super(OpType.CREATE_TABLE);
		this.tableName = tableName;
		this.meta = meta;
		this.subQuery = subQuery;
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	public Schema getTableMeta() {
		return this.meta;
	}
	
	public void setStoreType(StoreType type) {
		this.storeType = type;
	}
	
	public StoreType getStoreType() {
		return this.storeType;
	}
	
	public void setSubQuery(LogicalOp op) {
		this.subQuery = op;
	}
	
	public boolean hasSubQuery() {
		return this.subQuery != null;
	}
	
	public LogicalOp getSubQuery() {
		return this.subQuery;
	}

	/* (non-Javadoc)
	 * @see nta.query.plan.logical.LogicalOp#getSchema()
	 */
	@Override
	public Schema getSchema() {
		return null;
	}
}
