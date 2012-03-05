/**
 * 
 */
package tajo.index;

import java.io.IOException;

import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public abstract class IndexWriter {
  
  public abstract void write(Tuple key, long offset) throws IOException;
  
  public abstract void close() throws IOException;
}
