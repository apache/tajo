/**
 * 
 */
package nta.engine.exception;

/**
 * @author jihoon
 *
 */
public class UnknownWorkerException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -3677733092100608744L;

  public UnknownWorkerException() {
    
  }
  
  public UnknownWorkerException(String message) {
    super(message);
  }
  
  public UnknownWorkerException(Exception e) {
    super(e);
  }
  
  public UnknownWorkerException(String message, Exception e) {
    super(message, e);
  }
}
