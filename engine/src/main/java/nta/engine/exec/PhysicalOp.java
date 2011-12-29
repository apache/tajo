package nta.engine.exec;

import java.io.IOException;

import nta.engine.SchemaObject;
import nta.storage.Tuple;

public abstract class PhysicalOp implements SchemaObject {	
	public abstract Tuple next() throws IOException;	
}
