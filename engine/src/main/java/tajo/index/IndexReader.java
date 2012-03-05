package tajo.index;

import java.io.IOException;

import nta.storage.Tuple;

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
