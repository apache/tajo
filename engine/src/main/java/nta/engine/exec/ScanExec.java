package nta.engine.exec;

import java.io.IOException;

import nta.storage.Tuple;

public abstract class ScanExec {
	ScanExec outer;
	ScanExec inner;
	
	public ScanExec() {}
	
	public abstract Tuple next() throws IOException;	
}
