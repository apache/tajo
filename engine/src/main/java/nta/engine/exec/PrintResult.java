/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;

import nta.catalog.Schema;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 *
 */
public class PrintResult extends PhysicalOp {
	PhysicalOp sub;
	/**
	 * 
	 */
	public PrintResult(PhysicalOp op) {
		this.sub = op;
	}
	
	public VTuple next() throws IOException {
		VTuple next = sub.next();
		if(next == null)
			return null;
		else {
			System.out.println(next);
			return next;
		}
	}

	@Override
	public Schema getSchema() {
		return this.sub.getSchema();
	}
}
