package nta.storage;

import java.io.Closeable;
import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.ipc.protocolrecords.Tablet;

import org.apache.hadoop.conf.Configuration;

public interface FileScanner extends Closeable {

	public void init(Configuration conf, final Schema schema, final Tablet[] 
	    tablets) throws IOException;
	public Schema getSchema();	
	public Tuple next() throws IOException;
	public void reset() throws IOException;
	public void close() throws IOException;
}
