package nta.engine.executor;

import nta.catalog.Column;
import nta.storage.Tuple;

public class SortExec implements ScanExec {
	protected Tuple[] sortedScan;
	protected int currentRecord;

	public SortExec(ScanExec scan, Column[] cols, boolean desc) {
	}

	public boolean hasNext() {
		return false;
	}

	public Tuple next() {
		return null;
	}

}
