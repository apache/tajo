/**
 * 
 */
package nta.storage;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.SchemaObject;

/**
 * @author Hyunsik Choi
 *
 */
public interface Scanner extends SchemaObject {
	public abstract void init() throws IOException;

	abstract public Tuple next() throws IOException;	
	
	abstract public void reset() throws IOException;
	
	public abstract void close() throws IOException;
	
	public abstract Schema getSchema();
	
	public abstract boolean isLocalFile();
	public abstract boolean readOnly();
	public abstract boolean canRandomAccess();
}
