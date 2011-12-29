package nta.storage;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import nta.catalog.Schema;
import nta.conf.NtaConf;
import nta.engine.ipc.protocolrecords.Tablet;

public interface FileScanner extends Closeable {

	public void init(NtaConf conf, final Path path, final Schema schema, final Tablet[] tablets) throws IOException;
	public Tuple next() throws IOException;
	public void reset() throws IOException;
	public void close() throws IOException;
}
