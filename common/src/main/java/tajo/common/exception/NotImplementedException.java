/**
 * 
 */
package tajo.common.exception;

/**
 * @author Hyunsik Choi
 *
 */
public class NotImplementedException extends RuntimeException {

  /**
   * 
   */
  private static final long serialVersionUID = 8515328809349325243L;

  /**
   * 
   */
  public NotImplementedException() {
  }

  /**
   * @param message
   */
  public NotImplementedException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public NotImplementedException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public NotImplementedException(String message, Throwable cause) {
    super(message, cause);
  }
}
