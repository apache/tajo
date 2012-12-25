/**
 * 
 */
package tajo.storage.index;

import tajo.storage.Tuple;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public interface OrderIndexReader extends IndexReader {
  /**
   * Find the offset corresponding to key which is equal to or greater than 
   * a given key.
   * 
   * @param key to find
   * @return
   * @throws IOException 
   */
  public long find(Tuple key, boolean nextKey) throws IOException;
  
  /**
   * Return the next offset from the latest find or next offset
   * @return
   * @throws IOException
   */
  public long next() throws IOException;
}
