/**
 * 
 */
package nta.catalog;

import java.net.URI;

import nta.common.type.TimeRange;

/**
 * @author jimin
 *
 */
public class PartitionedTableDesc {

	private final TimeRange tsRange;
	private final int tsUnit;
	
	/**
	 * 
	 */
	public PartitionedTableDesc(int relId, String relName, Schema schema, URI uri, RelationType type, int rowSize, TimeRange tsRange, int tsUnit) {
		//super(relId, relName, schema, uri, type, rowSize);
		this.tsRange = tsRange;
		this.tsUnit = tsUnit;
	}
	
	public TimeRange getTimeRange() {
		return this.tsRange;
	}
	
	public int getTimeUnit() {
		return this.tsUnit;
	}

}
