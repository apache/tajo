/**
 * 
 */
package tajo.engine.exception;

/**
 * @author jihoon
 *
 */
public class NoSuchQueryIdException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -4425982532461186746L;

  public NoSuchQueryIdException() {
    
  }
  
  public NoSuchQueryIdException(String message) {
    super(message);
  }
  
  public NoSuchQueryIdException(Exception e) {
    super(e);
  }
  
  public NoSuchQueryIdException(String message, Exception e) {
    super(message, e);
  }
}
