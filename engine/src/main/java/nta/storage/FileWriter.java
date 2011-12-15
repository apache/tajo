package nta.storage;

import java.io.Closeable;
import java.io.IOException;

public interface FileWriter extends Closeable {

	public void addTuple() throws IOException;
	public void flush() throws IOException;
	public void close() throws IOException;
}
