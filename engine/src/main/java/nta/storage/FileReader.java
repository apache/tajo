package nta.storage;

import java.io.Closeable;
import java.io.IOException;

public interface FileReader extends Closeable {

	public Tuple next() throws IOException;
	public void reset() throws IOException;
	public void close() throws IOException;
}
