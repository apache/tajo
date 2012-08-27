/**
 * 
 */
package tajo.index;

import tajo.engine.exception.InternalException;

/**
 * @author Hyunsik Choi
 */
public class IndexKeyViolationExceotion extends InternalException {
  private static final long serialVersionUID = -4599687800634700759L;

  public IndexKeyViolationExceotion() {
  }

  /**
   * @param message
   */
  public IndexKeyViolationExceotion(String message) {
    super(message);
  }

  /**
   * @param message
   * @param t
   */
  public IndexKeyViolationExceotion(String message, Exception t) {
    super(message, t);
  }

  /**
   * @param t
   */
  public IndexKeyViolationExceotion(Exception t) {
    super(t);
  }
}
