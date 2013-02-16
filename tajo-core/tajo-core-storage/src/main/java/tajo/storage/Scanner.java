package tajo.storage;

import tajo.catalog.Column;
import tajo.catalog.SchemaObject;

import java.io.Closeable;
import java.io.IOException;

/**
 * Scanner Interface
 */
public interface Scanner extends SchemaObject, Closeable {

  void init() throws IOException;

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


  /**
   * It returns if the projection is executed in the underlying scanner layer.
   *
   * @return true if this scanner can project the given columns.
   */
  boolean isProjectable();

  /**
   * Set target columns
   * @param targets columns to be projected
   */
  void setTarget(Column [] targets);

  /**
   * It returns if the selection is executed in the underlying scanner layer.
   *
   * @return true if this scanner can filter tuples against a given condition.
   */
  boolean isSelectable();

  /**
   * Set a search condition
   * @param expr to be searched
   *
   * TODO - to be changed Object type
   */
  void setSearchCondition(Object expr);
}
