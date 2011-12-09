/**
 * 
 */
package nta.engine.plan.logical;

import nta.catalog.Schema;
import nta.catalog.proto.TableProtos.DataType;

/**
 * @author hyunsik
 *
 */
public class DescTableLO extends LogicalOp {
	Schema schema = null;
	Schema targetTable;
	
	/**
	 * @param opType
	 */
	public DescTableLO(Schema targetTable) {
		super(OpType.DESC_TABLE);
		this.targetTable = targetTable;
		this.schema = new Schema();
		this.schema.addColumn("field_id", DataType.INT);
		this.schema.addColumn("field_name", DataType.STRING);
		this.schema.addColumn("type", DataType.STRING);
	}
	
	public Schema getTargetTableMeta() {
		return this.targetTable;
	}

	/* (non-Javadoc)
	 * @see nta.query.plan.logical.LogicalOp#getSchema()
	 */
	@Override
	public Schema getSchema() {
		return schema;
	}
}
