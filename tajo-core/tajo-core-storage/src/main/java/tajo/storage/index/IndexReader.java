package tajo.storage.index;

import tajo.storage.Tuple;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public interface IndexReader {
  
  /**
   * Find the offset corresponding to key which is equal to a given key.
   * 
   * @param key
   * @return
   * @throws IOException 
   */
  public long find(Tuple key) throws IOException;
}
