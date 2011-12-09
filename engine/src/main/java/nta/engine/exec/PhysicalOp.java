package nta.engine.exec;

import java.io.IOException;

import nta.engine.SchemaObject;
import nta.storage.VTuple;

public abstract class PhysicalOp implements SchemaObject {	
	public abstract VTuple next() throws IOException;	
}
