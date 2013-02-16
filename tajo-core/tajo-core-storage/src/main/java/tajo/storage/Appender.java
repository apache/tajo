package tajo.storage;

import tajo.catalog.statistics.TableStat;

import java.io.Closeable;
import java.io.IOException;

public interface Appender extends Closeable {

  void init() throws IOException;

  void addTuple(Tuple t) throws IOException;
  
  void flush() throws IOException;
  
  void close() throws IOException;

  void enableStats();
  
  TableStat getStats();
}
