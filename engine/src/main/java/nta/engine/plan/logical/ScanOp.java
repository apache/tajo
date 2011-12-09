/**
 * 
 */
package nta.engine.plan.logical;

import nta.catalog.Schema;
import nta.engine.parser.NQL.RelInfo;

/**
 * @author Hyunsik Choi
 *
 */
public class ScanOp extends LogicalOp {
	RelInfo relInfo;
	
	public ScanOp(RelInfo relInfo) {
		super(OpType.SCAN);
		this.relInfo = relInfo;
	}

	public int getRelId() {
		return relInfo.getRelation().getId();
	}
	
	public String getRelName() {
		return relInfo.getName();
	}

	@Override
	public Schema getSchema() {
		return relInfo.getSchema();
	}
}
