/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;

import nta.catalog.Schema;
import nta.storage.Tuple;

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
	
	public Tuple next() throws IOException {
		Tuple next = sub.next();
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
