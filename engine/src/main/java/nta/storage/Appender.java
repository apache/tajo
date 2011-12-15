package nta.storage;

import java.io.Closeable;
import java.io.IOException;

public interface Appender extends Closeable {

	public void addTuple(Tuple t) throws IOException;
	public void flush() throws IOException;
	public void close() throws IOException;
}
