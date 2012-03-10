package nta.engine.planner.physical;

import java.io.IOException;

import nta.engine.SchemaObject;
import nta.storage.Tuple;

public abstract class PhysicalExec implements SchemaObject {	
	public abstract Tuple next() throws IOException;
	public abstract void rescan() throws IOException;
}
