/**
 * 
 */
package nta.engine.executor;

import java.io.IOException;

import nta.storage.Scanner;
import nta.storage.Tuple;
import nta.storage.UpdatableScanner;

/**
 * @author Hyunsik Choi
 *
 */
public class ResTableExec implements ScanExec {
	private final ScanExec scanner;
	private final UpdatableScanner resTable;
	/**
	 * 
	 */
	public ResTableExec(ScanExec scanExec, UpdatableScanner resTable) {
		this.scanner = scanExec;
		this.resTable = resTable;
	}

	/* (non-Javadoc)
	 * @see nta.query.executor.ScanExec#next()
	 */
	@Override
	public Tuple next() throws IOException {
		Tuple tuple = this.scanner.next();
		if(tuple == null)
			return null;
		
		this.resTable.addTuple(tuple);
		return tuple;
	}
}
