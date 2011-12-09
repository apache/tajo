package nta.engine.executor;

import java.io.IOException;

import nta.storage.Tuple;

public interface ScanExec {
	public Tuple next() throws IOException;	
}
