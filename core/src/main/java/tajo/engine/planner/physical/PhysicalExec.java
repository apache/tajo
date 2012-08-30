package tajo.engine.planner.physical;

import tajo.SchemaObject;
import tajo.storage.Tuple;

import java.io.IOException;

public abstract class PhysicalExec implements SchemaObject {
	public abstract Tuple next() throws IOException;
	public abstract void rescan() throws IOException;
}
