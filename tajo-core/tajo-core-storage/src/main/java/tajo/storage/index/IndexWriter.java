/**
 * 
 */
package tajo.storage.index;

import tajo.storage.Tuple;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public abstract class IndexWriter {
  
  public abstract void write(Tuple key, long offset) throws IOException;
  
  public abstract void close() throws IOException;
}
