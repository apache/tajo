/**
 * 
 */
package nta.engine.plan.logical;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;

/**
 * @author hyunsik
 *
 */
public class ControlLO extends LogicalOp {
	private Schema meta;
	
	/**
	 * @param opType
	 */
	public ControlLO(OpType opType) {
		super(opType);
		this.meta = new Schema();
		switch(opType) {
		case SHOW_TABLE: {
			this.meta = new Schema();
			this.meta.addColumn("table_name", DataType.STRING);
			break;
		}
		case SHOW_FUNCTION: {
			this.meta = new Schema();
			this.meta.addColumn("func_name", DataType.STRING);
			this.meta.addColumn("func_type", DataType.STRING);
			this.meta.addColumn("class", DataType.STRING);
		}
		}
	}

	/* (non-Javadoc)
	 * @see nta.query.plan.logical.LogicalOp#getSchema()
	 */
	@Override
	public Schema getSchema() {
		return meta;
	}

}
