package tajo.storage;

import tajo.catalog.SchemaObject;

import java.io.Closeable;
import java.io.IOException;

/**
 * Scanner Interface
 * 
 * @author hyunsik
 */
public interface Scanner extends SchemaObject, Closeable {
  /**
   * It returns one tuple at each call. 
   * 
   * @return retrieve null if the scanner has no more tuples. 
   * Otherwise it returns one tuple.
   * 
   * @throws IOException if internal I/O error occurs during next method
   */
  Tuple next() throws IOException;
  
  /**
   * Reset the cursor. After executed, the scanner 
   * will retrieve the first tuple.
   * 
   * @throws IOException if internal I/O error occurs during reset method
   */
  void reset() throws IOException;
  
  /**
   * Close scanner
   * 
   * @throws IOException if internal I/O error occurs during close method
   */
  void close() throws IOException;
}
