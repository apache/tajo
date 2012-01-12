package nta.storage;

import java.io.Closeable;
import java.io.IOException;

public interface Appender extends Closeable {
  
  public abstract void addTuple(Tuple t) throws IOException;
  
  public abstract void flush() throws IOException;
  
  public abstract void close() throws IOException;
}
